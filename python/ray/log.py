import datetime
import functools
import inspect
import logging
import os
import pathlib
import pickle
import re
import socketserver
import struct
from logging.config import dictConfig
from typing import Callable, Iterable, List, Optional, Union

# Set the number of columns to use for rich output when in jupyter
os.environ["JUPYTER_COLUMNS"] = "160"

try:
    import rich
    import rich.logging

    FormatTimeCallable = Callable[[datetime.datetime], rich.text.Text]

    class RayLogRender(rich._log_render.LogRender):
        """Render a log record (renderables) to a rich console.

        This class stores some console state; for example, the last logged time, which
        is used in formatting subsequent messages.
        """

        def __call__(
            self,
            console: rich.console.Console,
            renderables: Iterable[rich.console.ConsoleRenderable],
            log_time: Optional[datetime.datetime] = None,
            time_format: Optional[Union[str, FormatTimeCallable]] = None,
            level: rich.text.TextType = "",
            path: Optional[str] = None,
            line_no: Optional[int] = None,
            link_path: Optional[str] = None,
            package: Optional[str] = None,
        ) -> rich.table.Table:
            """Render a log message to the rich console.

            console: Console to write the message to
            renderables: A list containing either 1 or 2 items: the log message and
                optionally a traceback

            Returns
            -------
                A table contianing the log message.
            """
            output = rich.table.Table.grid(padding=(0, 1), expand=True)

            if self.show_time:
                output.add_column(style="log.time")

            # Ray package column
            output.add_column(style="yellow")

            if self.show_level:
                output.add_column(style="log.level", width=self.level_width)

            output.add_column(ratio=1, style="log.message", overflow="fold")

            if self.show_path and path:
                output.add_column(style="log.path", justify="right")

            row: List[rich.console.RenderableType] = []
            if self.show_time:
                log_time = log_time or console.get_datetime()
                time_format = time_format or self.time_format
                if callable(time_format):
                    log_time_display = time_format(log_time)
                else:
                    log_time_display = rich.text.Text(log_time.strftime(time_format))

                if log_time_display == self._last_time and self.omit_repeated_times:
                    row.append(rich.text.Text(" " * len(log_time_display)))
                else:
                    row.append(log_time_display)
                    self._last_time = log_time_display

            if package:
                row.append(rich.text.Text(f"[{package}]"))
            else:
                row.append(rich.text.Text(""))

            if self.show_level:
                row.append(level)

            row.append(rich.containers.Renderables(renderables))
            if self.show_path and path:
                path_text = rich.text.Text()
                path_text.append(
                    path, style=f"link file://{link_path}" if link_path else ""
                )
                if line_no:
                    path_text.append(":")
                    path_text.append(
                        f"{line_no}",
                        style=f"link file://{link_path}#{line_no}" if link_path else "",
                    )
                row.append(path_text)

            output.add_row(*row)
            return output

    class RichRayHandler(logging.handlers.SocketHandler, rich.logging.RichHandler):
        """Rich log handler, used to handle logs if rich is installed."""

        def __init__(
            self,
            *args,
            show_time: bool = True,
            omit_repeated_times: bool = True,
            show_level: bool = True,
            show_path: bool = True,
            highlighter: rich.highlighter.Highlighter = None,
            log_time_format: Union[str, FormatTimeCallable] = "[%x %X]",
            **kwargs,
        ):
            # Rich highlighter conflicts with colorama; disable here
            if not highlighter:
                highlighter = rich.highlighter.NullHighlighter()

            logging.handlers.SocketHandler.__init__(self, None, None)
            rich.logging.RichHandler.__init__(
                self, *args, highlighter=highlighter, rich_tracebacks=False, **kwargs
            )

            self._log_render = RayLogRender(
                show_time=show_time,
                show_level=show_level,
                show_path=show_path,
                time_format=log_time_format,
                omit_repeated_times=omit_repeated_times,
                level_width=None,
            )

        def emit(self, record: logging.LogRecord):
            """Emit the log message.

            If this is a worker, serialize the record and send it to the driver via TCP.
            If this is the driver, emit the message using the rich handler.

            Args:
                record: Log record to be emitted
            """
            import ray

            if (
                ray._private.worker.global_worker.mode
                == ray._private.worker.WORKER_MODE
            ):
                if self.port is None:
                    self.address = (
                        "localhost",
                        logging.handlers.DEFAULT_TCP_LOGGING_PORT,
                    )
                    self.port = logging.handlers.DEFAULT_TCP_LOGGING_PORT

                logging.handlers.SocketHandler.emit(self, record)

            else:
                rich.logging.RichHandler.emit(self, record)

        def render(
            self,
            *,
            record: logging.LogRecord,
            traceback: Optional[rich.traceback.Traceback],
            message_renderable: rich.console.ConsoleRenderable,
        ) -> rich.console.ConsoleRenderable:
            """Render log for display.

            Args:
                record: logging Record.
                traceback: Traceback instance or None for no Traceback.
                message_renderable: Renderable (typically Text) containing log message
                    contents.

            Returns:
                Renderable to display log.
            """
            path = pathlib.Path(record.pathname).name
            level = self.get_level_text(record)
            time_format = None if self.formatter is None else self.formatter.datefmt
            log_time = datetime.datetime.fromtimestamp(record.created)

            log_renderable = self._log_render(
                self.console,
                [message_renderable]
                if not traceback
                else [message_renderable, traceback],
                log_time=log_time,
                time_format=time_format,
                level=level,
                path=path,
                line_no=record.lineno,
                link_path=record.pathname if self.enable_link_path else None,
                package=record.package,
            )
            return log_renderable

except ImportError:
    rich = None
    logging.warn(
        "rich is not installed. Run `pip install rich` for"
        " improved logging, progress, and tracebacks."
    )


class ContextFilter(logging.Filter):
    """A filter that adds ray context info to log records.

    This filter adds a package name to append to the message as well as information
    about what worker emitted the message, if applicable.
    """

    logger_regex = re.compile(r"ray(\.(?P<subpackage>\w+))?(\..*)?")
    package_message_names = {
        "air": "AIR",
        "data": "Data",
        "rllib": "RLlib",
        "serve": "Serve",
        "train": "Train",
        "tune": "Tune",
        "workflow": "Workflow",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.global_worker_mode = None

    def filter(self, record: logging.LogRecord):
        match = self.logger_regex.search(record.name)
        if match:
            record.package = (
                f"Ray {self.package_message_names.get(match['subpackage'], 'Core')}"
            )
        else:
            record.package = "Ray Core"

        # Lazily load ray when this is called to avoid circular import
        import ray

        if self.global_worker_mode is None:
            self.global_worker_mode = ray._private.worker.global_worker.mode

        if self.global_worker_mode == ray._private.worker.WORKER_MODE:
            record.node_ip_address = (
                ray.runtime_context.get_runtime_context().worker.node_ip_address
            )

        return True


class AnsiStripFormatter(logging.Formatter):
    """Formatter which strips ANSI escape codes from log messages.

    These escape codes produce noise in output files and conflict with rich logging,
    and are therefore stripped here.
    """

    strip_ansi_regex = re.compile(r"\x1b\[[0-9;]*m")

    def format(self, record):
        return re.sub(self.strip_ansi_regex, "", super().format(record))


def oneshot(func: Callable[[None], None]):
    """Decorator for a function that should only run once.

    Args:
        func: Function to wrap.

    Returns:
        A function that only runs once; other calls generate debug messages and do
        not run.
    """

    @functools.wraps(func)
    def wrapped(force=False):
        caller = inspect.stack()[-1]

        if not (wrapped.last_caller or force):
            wrapped.last_caller = caller
            func()
        else:
            logging.getLogger(__name__).debug(
                "Ray logging has already been configured at "
                f"{wrapped.last_caller.filename}::{wrapped.last_caller.lineno}."
                " Skipping reconfiguration requested at "
                f"{caller.filename}::{caller.lineno}."
            )

    wrapped.last_caller = None
    return wrapped


class PlainRayHandler(logging.handlers.SocketHandler, logging.StreamHandler):
    """Plain log handler, used as fallback if rich is not installed."""

    def __init__(self):
        logging.handlers.SocketHandler.__init__(self, None, None)
        logging.StreamHandler.__init__(self)

    def emit(self, record: logging.LogRecord):
        """Emit the log message.

        If this is a worker, serialize the record and send it to the driver via TCP.
        If this is the driver, emit the message using the appropriate console handler.

        Args:
            record: Log record to be emitted
        """
        #
        # by the LogRecordStreamHandler running in a thread there.
        import ray

        if ray._private.worker.global_worker.mode == ray._private.worker.WORKER_MODE:
            if self.port is None:
                self.address = ("localhost", logging.handlers.DEFAULT_TCP_LOGGING_PORT)
                self.port = logging.handlers.DEFAULT_TCP_LOGGING_PORT

            logging.handlers.SocketHandler.emit(self, record)

        else:
            logging.StreamHandler.emit(self, record)


class LogRecordStreamer(socketserver.StreamRequestHandler):
    """Helper for streaming logging requests with the LogRecordReceiver.

    This logs the record using whatever logging policy is
    configured locally.

    See https://docs.python.org/3/howto/logging-cookbook.html
        #sending-and-receiving-logging-events-across-a-network
    for more information.
    """

    def handle(self):
        """Handle multiple requests - each expected to be a 4-byte length,
        followed by the LogRecord in pickle format. Logs the record
        according to whatever policy is configured locally.
        """
        while True:
            chunk = self.connection.recv(4)
            if len(chunk) < 4:
                break
            slen = struct.unpack(">L", chunk)[0]
            chunk = self.connection.recv(slen)
            while len(chunk) < slen:
                chunk = chunk + self.connection.recv(slen - len(chunk))
            obj = self.unPickle(chunk)
            record = logging.makeLogRecord(obj)
            self.handleLogRecord(record)

    def unPickle(self, data):
        return pickle.loads(data)

    def handleLogRecord(self, record):
        # if a name is specified, we use the named logger rather than the one
        # implied by the record.
        if self.server.logname is not None:
            name = self.server.logname
        else:
            name = record.name

        record.is_from_worker = True
        logger = logging.getLogger(name)
        # N.B. EVERY record gets logged. This is because Logger.handle
        # is normally called AFTER logger-level filtering. If you want
        # to do filtering, do it at the client end to save wasting
        # cycles and network bandwidth!
        logger.handle(record)


class LogRecordReceiver(socketserver.ThreadingTCPServer):
    """Simple TCP socket-based logging receiver.

    This is a TCP server which listens for traffic. Once a message is received,
    it is passed to the LogRecordStreamer, which deserializes the message
    as a LogRecord and then emits the message.

    See https://docs.python.org/3/howto/logging-cookbook.html
        #sending-and-receiving-logging-events-across-a-network
    for more information.
    """

    allow_reuse_address = True

    def __init__(
        self,
        host="localhost",
        port=logging.handlers.DEFAULT_TCP_LOGGING_PORT,
        handler=LogRecordStreamer,
    ):
        socketserver.ThreadingTCPServer.__init__(self, (host, port), handler)
        self.abort = 0
        self.timeout = 1
        self.logname = None

    def serve_until_stopped(self):
        """Listen for TCP traffic, then handle any requests received."""
        import select

        abort = 0
        while not abort:
            rd, wr, ex = select.select([self.socket.fileno()], [], [], self.timeout)
            if rd:
                self.handle_request()
            abort = self.abort

    @classmethod
    def start_log_receiver(
        cls,
        host: str = "localhost",
        port: int = logging.handlers.DEFAULT_TCP_LOGGING_PORT,
    ):
        """Start the LogRecordReceiver to listen for TCP logging traffic.

        Args:
            host: The address on which the server is listening
            port: The port on which the server is listening
        """
        server = cls(host, port)
        server.serve_until_stopped()


@oneshot
def generate_logging_config():
    """Generate the default Ray logging configuration.

    If rich is installed, a logger which generates output using the rich console is
    used. Otherwise, a simple fallback logging format is used, and a warning message
    is emitted asking the user to install rich for better formatting.

    In either case, if the handler is run on a worker, it serializes the LogRecord
    and sends it to the driver via TCP. If the handler is run on the driver, it is
    emitted as usual.
    """
    formatters = {
        "rich": {
            "()": AnsiStripFormatter,
            "datefmt": "[%Y-%m-%d %H:%M:%S]",
            "format": "%(message)s",
        },
        "plain": {
            "datefmt": "[%Y-%m-%d %H:%M:%S]",
            "format": "%(asctime)s [%(package)s] %(levelname)s %(name)s::%(message)s",
        },
    }
    filters = {"context_filter": {"()": ContextFilter}}
    handlers = {
        "null": {"class": "logging.NullHandler"},
    }
    if rich:
        handlers["default"] = {
            "()": RichRayHandler,
            "formatter": "rich",
            "filters": ["context_filter"],
        }
    else:
        handlers["default"] = {
            "()": PlainRayHandler,
            "formatter": "plain",
            "filters": ["context_filter"],
        }

    loggers = {
        # Default ray logger; any log message that gets propagated here will be logged
        # to the console
        "ray": {
            "level": "INFO",
            "handlers": ["default"],
        },
        # Special handling for ray.rllib: only warning-level messages passed through
        # See https://github.com/ray-project/ray/pull/31858 for related PR
        "ray.rllib": {
            "level": "WARN",
        },
    }

    dictConfig(
        {
            "version": 1,
            "formatters": formatters,
            "filters": filters,
            "handlers": handlers,
            "loggers": loggers,
        }
    )

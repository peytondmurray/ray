import datetime
import functools
import inspect
import logging
import os
import pathlib
import re
from logging.config import dictConfig
from typing import Callable, Iterable, List, Optional, Union

import ray

os.environ["JUPYTER_COLUMNS"] = "160"

DEFAULT_DATASET_LOG_PATH = "logs/ray-data.log"

try:
    import rich
    import rich.logging

    FormatTimeCallable = Callable[[datetime.datetime], rich.text.Text]

    class RayLogRender(rich._log_render.LogRender):
        """Render a log record (renderables) to a rich console."""

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

    class ConsoleHandler(rich.logging.RichHandler):
        """Logging handler which uses rich to produce nicely formatted logs."""

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
            if not highlighter:
                highlighter = rich.highlighter.NullHighlighter()

            super().__init__(*args, highlighter=highlighter, **kwargs)
            self._log_render = RayLogRender(
                show_time=show_time,
                show_level=show_level,
                show_path=show_path,
                time_format=log_time_format,
                omit_repeated_times=omit_repeated_times,
                level_width=None,
            )

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
    logging.info(
        "rich is not installed. Run `pip install rich` for"
        " improved logging, progress, and tracebacks."
    )


class ContextFilter(logging.Filter):
    """A filter that adds info about the relevant ray package to log records."""

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

    def filter(self, record: logging.LogRecord):
        match = self.logger_regex.search(record.name)
        if match:
            record.package = (
                f"Ray {self.package_message_names.get(match['subpackage'], 'Core')}"
            )
        else:
            record.package = "Ray Core"

        return True


class AnsiStripFormatter(logging.Formatter):
    """Formatter which strips ANSI escape codes from log messages.

    These escape codes produce noise in output files and conflict with rich logging,
    and are therefore stripped here.
    """

    strip_ansi_regex = re.compile(r"\x1b\[[0-9;]*m")

    def format(self, record):
        return re.sub(self.strip_ansi_regex, "", super().format(record))


class SessionRotatingFileHandler(logging.Handler):
    """A rotating log handler that logs to a log directory for the session.

    In contrast to a normal logging.RotatingFileHandler, this handler will only open
    a log file if the global node session directory path is valid, i.e. if
    ray._private.worker._global_node is defined.
    """

    def __init__(
        self,
        logs_dir,
        mode="a",
        maxBytes=0,
        backupCount=0,
        encoding=None,
        delay=False,
        errors=None,
    ):
        super().__init__()
        self.logs_dir = pathlib.Path(logs_dir)
        self.mode = mode
        self.maxBytes = maxBytes
        self.backupCount = backupCount
        self.encoding = encoding
        self.delay = delay
        self.errors = errors
        self.handler = None

    def emit(self, record):
        if not self.handler:
            global_node = ray._private.worker._global_node
            if global_node is not None:
                session_dir = pathlib.Path(global_node.get_session_dir_path())
                self.handler = logging.handlers.RotatingFileHandler(
                    session_dir / self.logs_dir,
                    mode=self.mode,
                    maxBytes=self.maxBytes,
                    backupCount=self.backupCount,
                    encoding=self.encoding,
                    delay=self.delay,
                    errors=self.errors,
                )

        if self.handler:
            self.handler.emit(record)

    def flush(self):
        if self.handler:
            self.handler.flush()

    def close(self):
        if self.handler:
            self.handler.close()


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


@oneshot
def generate_logging_config():
    """Generate the default Ray logging configuration.

    If rich is installed, a logger which generates output using the rich console is
    used. Otherwise, a simple fallback logging format is used, and a warning message
    is emitted asking the user to install rich for better formatting.

    Args:
        force: Force reconfiguration of the logging system.
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

    handlers = {}
    if rich:
        handlers["console"] = {
            "()": ConsoleHandler,
            "filters": ["context_filter"],
            "formatter": "rich",
        }
    else:
        handlers["console"] = {
            "class": "logging.StreamHandler",
            "filters": ["context_filter"],
            "formatter": "plain",
        }

    handlers["dataset_file"] = {
        "()": SessionRotatingFileHandler,
        "filters": ["context_filter"],
        "formatter": "plain",
        "logs_dir": pathlib.Path(DEFAULT_DATASET_LOG_PATH),
    }

    loggers = {
        # Default ray logger; any log message that gets propagated here will be logged
        # to the console
        "ray": {
            "level": "INFO",
            "handlers": ["console"],
        },
        # Special handling for ray.data: log to a file before propagating up to parent
        # logger. See https://github.com/ray-project/ray/issues/29575 for discussion
        # "ray.data": {
        #     "level": "INFO",
        #     "handlers": ["dataset_file", "plain"],
        #     "propagate": False,
        # },
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

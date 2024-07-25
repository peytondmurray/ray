import re
import importlib

from enum import Enum
from dataclasses import dataclass
from typing import Optional, List, Tuple, Set, Dict


_SPHINX_AUTOSUMMARY_HEADER = ".. autosummary::"
_SPHINX_AUTOCLASS_HEADER = ".. autoclass::"
# This is a special character used in autosummary to render only the api shortname, for
# example ~module.api_name will render only api_name
_SPHINX_AUTODOC_SHORTNAME = "~"


class AnnotationType(Enum):
    PUBLIC_API = "PublicAPI"
    DEVELOPER_API = "DeveloperAPI"
    DEPRECATED = "Deprecated"
    UNKNOWN = "Unknown"


class CodeType(Enum):
    CLASS = "Class"
    FUNCTION = "Function"


@dataclass
class API:
    name: str
    annotation_type: AnnotationType
    code_type: CodeType

    @staticmethod
    def from_autosummary(doc: str, current_module: Optional[str] = None) -> List["API"]:
        """
        Parse API from the following autosummary sphinx block.

        .. autosummary::
            :option_01
            :option_02

            api_01
            api_02
        """
        apis = []
        lines = doc.splitlines()
        if not lines:
            return apis

        if lines[0].strip() != _SPHINX_AUTOSUMMARY_HEADER:
            return apis

        for line in lines:
            if line == _SPHINX_AUTOSUMMARY_HEADER:
                continue
            if line.strip().startswith(":"):
                # option lines
                continue
            if not line.strip():
                # empty lines
                continue
            if not re.match(r"\s", line):
                # end of autosummary, \s means empty space, this line is checking if
                # the line is not empty and not starting with empty space
                break
            attribute = line.strip().removeprefix(_SPHINX_AUTODOC_SHORTNAME)
            api_name = f"{current_module}.{attribute}" if current_module else attribute
            apis.append(
                API(
                    name=api_name,
                    annotation_type=AnnotationType.PUBLIC_API,
                    code_type=CodeType.FUNCTION,
                )
            )

        return apis

    @staticmethod
    def from_autoclass(
        doc: str, current_module: Optional[str] = None
    ) -> Optional["API"]:
        """
        Parse API from the following autoclass sphinx block.

        .. autoclass:: api_01
        """
        doc = doc.strip()
        if not doc.startswith(_SPHINX_AUTOCLASS_HEADER):
            return None
        cls = (
            doc[len(_SPHINX_AUTOCLASS_HEADER) :]
            .strip()
            .removeprefix(_SPHINX_AUTODOC_SHORTNAME)
        )
        api_name = f"{current_module}.{cls}" if current_module else cls

        return API(
            name=api_name,
            annotation_type=AnnotationType.PUBLIC_API,
            code_type=CodeType.CLASS,
        )

    def get_canonical_name(self) -> str:
        """
        Some APIs have aliases declared in __init__.py file (see ray/data/__init__.py
        for example). This method converts the alias to full name. This is to make sure
        out analysis can be performed on the same set of canonial names.
        """
        modname, _, objname = self.name.rpartition(".")
        try:
            module = importlib.import_module(modname)
            obj = getattr(module, objname)
            return f"{obj.__module__}.{obj.__qualname__}"
        except (ModuleNotFoundError, AttributeError):
            pass

        try:
            modname, _, clsname = modname.rpartition(".")
            module = importlib.import_module(modname)
            cls = getattr(module, clsname)
            meth = getattr(cls, objname)
            return f"{cls.__module__}.{cls.__qualname__}.{meth.__name__}"
        except (ModuleNotFoundError, AttributeError):
            return self.name

    def has_private_name(self) -> bool:
        """
        Check if this API has a private name. Private names are those that start with
        underscores.
        """
        name_has_underscore = self.name.split(".")[-1].startswith("_")
        is_internal = "._internal." in self.name

        return name_has_underscore or is_internal

    def is_public(self) -> bool:
        """
        Check if this API is public. Public APIs are those that are annotated as public
        and not have private names.
        """
        return self.annotation_type == AnnotationType.PUBLIC_API

    def is_developer(self) -> bool:
        return self.annotation_type == AnnotationType.DEVELOPER_API

    def is_deprecated(self) -> bool:
        """
        Check if this API is deprecated. Deprecated APIs are those that are annotated as
        deprecated.
        """
        return self.annotation_type == AnnotationType.DEPRECATED

    def __hash__(self):
        return hash(self.get_canonical_name())

    def __eq__(self, other):
        return self.get_canonical_name() == other.get_canonical_name()

    def __str__(self):
        return self.get_canonical_name()

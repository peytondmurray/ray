import pathlib
import re
import sys
import textwrap
from typing import Dict, List, Set, Tuple

from sphinx.application import Sphinx

sys.path.insert(0, str(pathlib.Path(__file__).parent))
sys.path.insert(
    0, str(pathlib.Path(__file__).parent.parent.parent.parent)
)  # Base of the repo

from api import API  # noqa: E402
from module import Module  # noqa: E402


def _validate_documented_public_apis(
    api_in_codes: Dict[str, API],
    api_in_docs: Dict[str, API],
    white_list_apis: Set[str],
) -> Tuple[Set[str], ...]:
    """
    Validate APIs that are public and documented.
    """
    good_apis = set()
    private_documented_apis = set()
    deprecated_documented_apis = set()
    undocumented_public_apis = set()
    undocumented_deprecated_public_apis = set()
    public_but_private_name = set()

    for code_api_name, api in api_in_codes.items():
        if api.is_public():
            if code_api_name in api_in_docs:
                good_apis.add(api)
            else:
                if api.has_private_name():
                    public_but_private_name.add(api)

                undocumented_public_apis.add(api)
        elif api.is_developer():
            continue
        elif api.is_deprecated():
            if code_api_name in api_in_docs:
                deprecated_documented_apis.add(api)
            else:
                undocumented_deprecated_public_apis.add(api)
        else:
            if code_api_name in api_in_docs:
                private_documented_apis.add(api)

    return (
        good_apis,
        private_documented_apis,
        deprecated_documented_apis,
        undocumented_public_apis,
        undocumented_deprecated_public_apis,
        public_but_private_name,
    )


def get_all_docs() -> list[pathlib.Path]:
    """Get a list of all rst/md/ipynb files ingested by sphinx.

    Returns
    -------
    list[pathlib.Path]

    """
    root_dir = pathlib.Path(__file__).parent.parent.parent.parent.resolve()
    doc_source = root_dir / "doc" / "source"
    app = Sphinx(doc_source, None, "/tmp/output/", "/tmp/doctrees/", "html")

    docs = []
    extensions = [".md", ".rst", ".ipynb"]
    for doc in app.env.found_docs:

        if doc.startswith("_templates"):
            continue

        for ext in extensions:
            file = (doc_source / doc).with_suffix(ext)
            if file.exists():
                docs.append(file)
                break

    return docs


def get_autodoc_apis(docs: list[pathlib.Path]) -> dict[pathlib.Path, list[API]]:
    autodoc_apis = {}
    for doc in docs:
        autodoc_apis[doc] = parse_autodoc_directives(doc)

    return autodoc_apis


def parse_autodoc_directives(rst_file: pathlib.Path) -> List[API]:
    """Parse the rst file to find the autodoc APIs.

    Example content of the rst file:

        .. currentmodule:: mymodule

        .. autoclass:: myclass

        .. autosummary::

            myclass.myfunc_01
            myclass.myfunc_02
    """
    _SPHINX_CURRENTMODULE_HEADER = ".. currentmodule::"
    # _SPHINX_TOCTREE_HEADER = ".. toctree::"
    # _SPHINX_INCLUDE_HEADER = ".. include::"
    _SPHINX_AUTOSUMMARY_HEADER = ".. autosummary::"
    _SPHINX_AUTOCLASS_HEADER = ".. autoclass::"

    if not rst_file.exists():
        return []

    apis = []
    module = None
    with open(rst_file, "r") as f:
        line = f.readline()
        while line:
            # parse currentmodule block
            if line.startswith(_SPHINX_CURRENTMODULE_HEADER):
                module = line[len(_SPHINX_CURRENTMODULE_HEADER) :].strip()

            # parse autoclass block
            if line.startswith(_SPHINX_AUTOCLASS_HEADER):
                apis.append(API.from_autoclass(line, module))

            # parse autosummary block
            if line.startswith(_SPHINX_AUTOSUMMARY_HEADER):
                doc = line
                line = f.readline()
                # collect lines until the end of the autosummary block
                while line:
                    doc += line
                    if line.strip() and not re.match(r"\s", line):
                        # end of autosummary, \s means empty space, this line is
                        # checking if the line is not empty and not starting with
                        # empty space
                        break
                    line = f.readline()

                apis.extend(API.from_autosummary(doc, module))
                continue

            line = f.readline()

    return [api for api in apis if api]


def sort_format(items):
    return textwrap.indent(
        "\n".join(sorted(str(item) for item in items)),
        "  ",
    )


def main():
    modules = {
        # 'data': ['ray.data', 'ray.data.grouped_data'],
        # 'train': ['ray.train'],
        "tune": ["ray.tune"],
    }
    docs = get_all_docs()
    apis = get_autodoc_apis(docs)

    apis_in_docs = {}
    for doc, doc_apis in apis.items():
        for api in doc_apis:
            apis_in_docs[api.get_canonical_name()] = api

    apis_in_code = {}
    for library, lib_modules in modules.items():
        for module in lib_modules:
            for api in Module(module).get_apis():
                apis_in_code[api.get_canonical_name()] = api

    (
        good,
        private_documented,
        deprecated_documented,
        undocumented_public,
        undocumented_deprecated_public,
        public_but_private_name,
    ) = _validate_documented_public_apis(apis_in_code, apis_in_docs, {})

    print("Good APIs:")
    print(sort_format(good))
    print("")
    print("Private but documented APIs:")
    print(sort_format(private_documented))
    print("")
    print("Deprecated but documented APIs:")
    print(sort_format(deprecated_documented))
    print("")
    print("Undocumented Public APIs:")
    print(sort_format(undocumented_public))
    print("")
    print("Undocumented Deprecated Public APIs:")
    print(sort_format(undocumented_deprecated_public))
    print("")
    print("Public APIs with private names")
    print(sort_format(public_but_private_name))


if __name__ == "__main__":
    main()

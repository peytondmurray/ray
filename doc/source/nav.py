from typing import Dict, Union, List
NavEntry = Dict[str, Union[str, List["NavEntry"]]]

class Nav:

    def __init__(self, tree: List[NavEntry]):
        self.tree = tree
        self.flat = self.to_flat(self.tree)

    def __contains__(self, item) -> bool:
        return item in self.flat

    def __setitem__(self, key, value):
        self.flat[key] = value

    # reversed list of toctree ancestors
    # ancestors = [
    #     'tune/index'
    #     'tune/api/api',
    #     'tune/api/suggestion',
    #     'tune/api/doc/ray.tune.search.hebo.HEBOSearch',
    #     'tune/api/doc/ray.tune.search.hebo.HEBOSearch.save',
    # ]
    def insert(self, ancestors: List[str]):
        for i, ancestor in enumerate(ancestors):
            if ancestor not in self:
                self[ancestor] = {'file': ancestor}

                if i == 0:
                    raise ValueError(f"Top level ancestor of page must exist: {ancestor}")
                else:
                    # Insert the new ancestor into the parent NavEntry's list of subsections;
                    # add a 'sections' if needed
                    parent = self[ancestors[i-1]]
                    if 'sections' not in parent:
                        parent['sections'] = []

                    parent['sections'].append(self[ancestor])

    def __getitem__(self, key):
        return self.flat[key]

    def to_flat(self, navs: List[NavEntry]) -> Dict[str, NavEntry]:
        flat = {}
        for nav in navs:
            flat[nav['file']] = nav

            if 'sections' in nav:
                flat.update(self.to_flat(nav['sections']))

        return flat

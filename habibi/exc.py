"""
    habibi.exc
    ~~~~~~~~~~

    Module contains exception classes, that habibi package uses and raises.
    Habibi code should only raise exceptions, inherited from HabibiException class.
"""


class HabibiException(Exception):
    pass

class HabibiApiException(HabibiException):
    pass

class HabibiNotFound(HabibiException):
    pass

class HabibiModelNotFound(HabibiNotFound):
    def __init__(self, model_name):
        self.model_name = model_name

    def __str__(self):
        return 'Habibi DB model does not exist. name="{}"'.format(self.model_name)

class HabibiApiNotFound(HabibiNotFound):
    def __init__(self, model, ids, kwargs):

        self.model = model
        self.ids = ids and [str(_id) for _id in ids] or list()
        self.kwargs = kwargs or dict()

    def __str__(self):
        what = self.model.__name__ + 's'
        search_conds = ""

        if 1 == len(self.ids):
            search_conds += "id is {}".format(self.ids[0])
        elif len(self.ids) > 1:
            search_conds += "ids in [{}]".format(", ".join(self.ids))

        if self.kwargs:
            search_conds += (", " + ", ".join(["{}={}".format(k,v)
                                              for k,v in self.kwargs.items()]))

        return '{what} were not found in DB. Search conditions: {conds}.'.format(
            what=what, conds=search_conds)


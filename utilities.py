class StringMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.__dict__)

    def __str__(self):
        return repr(self)

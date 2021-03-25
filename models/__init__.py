import copy

from .fields import *

NO_VALUE = '<NO_VALUE>'


class Model():
    def __init__(self, *args, **kwargs):
        self.original_data = self.on_receive_data(kwargs)
        for prop in dir(self):
            if prop.startswith('__'):
                continue
            attr = getattr(self, prop)
            if not isinstance(attr, BaseField):
                continue
            attr.name = prop
            value = kwargs.get(attr.rel, NO_VALUE)
            if value == NO_VALUE:
                continue
            setattr(self, prop, value)
        self.on_populate_finish(self.original_data)

    def __setattr__(self, prop, value):
        try:
            attr = getattr(self, prop)
        except AttributeError:
            return super().__setattr__(prop, value)
        if isinstance(attr, BaseField):
            field = copy.deepcopy(attr)
            field._received_value = value
            def on_set(x, _, __): return x
            on_set = getattr(self, f'on_{prop}', on_set)
            field.value = field.on_set(on_set(value, field, self.original_data), self.original_data)
            return super().__setattr__(prop, field)
        return super().__setattr__(prop, value)

    def serialize(self):
        data = {}
        for prop in dir(self):
            if prop.startswith('__'):
                continue
            field = getattr(self, prop)
            if not isinstance(field, BaseField):
                continue
            data[field.rel.lower()] = field.serialize()
        return data

    @classmethod
    def fieldnames(self):
        data = []
        for prop in dir(self):
            if prop.startswith('__'):
                continue
            field = getattr(self, prop)
            if not isinstance(field, BaseField):
                continue
            name = field.rel or prop
            data.append(name.lower())
        return data

    def on_receive_data(self, *args, **kwargs):
        pass

    def on_populate_finish(self, *args, **kwargs):
        pass

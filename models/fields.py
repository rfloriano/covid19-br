import rows


class BrDateSerializer(rows.fields.DateField):
    INPUT_FORMAT = "%d/%m/%Y"

    @classmethod
    def deserialize(cls, value):
        if not (value or "").strip():
            return None
        elif value.count("/") == 2 and len(value.split("/")[-1]) == 2:
            parts = value.split("/")
            value = f"{parts[0]}/{parts[1]}/20{parts[2]}"
        return super().deserialize(value)


class BaseField(object):
    SERIALIZER = rows.fields.Field

    def __init__(self, rel='', help='', nullable=False, default=None):
        self._rel = rel
        self.name = None
        self.help = help
        self.nullable = nullable
        self.default = default
        self._value = default
        self._received_value = default
        self.check_values()

    def __repr__(self):
        return repr(self.value)

    def __lt__(self, other):
        if isinstance(other, BaseField):
            other = other.value
        return self.value < other

    def __le__(self, other):
        if isinstance(other, BaseField):
            other = other.value
        return self.value <= other

    def __gt__(self, other):
        if isinstance(other, BaseField):
            other = other.value
        return self.value > other

    def __ge__(self, other):
        if isinstance(other, BaseField):
            other = other.value
        return self.value >= other

    def __eq__(self, other):
        if isinstance(other, BaseField):
            other = other.value
        return self.value == other

    def __ne__(self, other):
        if isinstance(other, BaseField):
            other = other.value
        return self.value != other

    def __add__(self, other):
        if isinstance(other, BaseField):
            other = other.value
        return self.value + other

    def __sub__(self, other):
        if isinstance(other, BaseField):
            other = other.value
        return self.value - other

    def __mul__(self, other):
        if isinstance(other, BaseField):
            other = other.value
        return self.value * other

    def __div__(self, other):
        if isinstance(other, BaseField):
            other = other.value
        return self.value / other

    def __mod__(self, other):
        if isinstance(other, BaseField):
            other = other.value
        return self.value % other

    def __pow__(self, other):
        if isinstance(other, BaseField):
            other = other.value
        return self.value ** other

    def __hash__(self):
        return hash(self.value)

    def on_set(self, value, all_data):
        return value

    @property
    def value(self):
        return self._value

    @property
    def rel(self):
        return self._rel or self.name

    @value.setter
    def value(self, new_value):
        self.validate(new_value)
        self._value = self.deserialize(new_value)

    def validate(self, value):
        if not self.nullable and value is None:
            raise ValueError(f'Invalid value for field "{self.rel}": {value} for a not nullable field')

    def check_values(self):
        pass

    def serialize(self, *args, **kwargs):
        return self.SERIALIZER.serialize(self.value, *args, **kwargs)

    def deserialize(self, value, *args, **kwargs):
        return self.SERIALIZER.deserialize(value, *args, **kwargs)


class BrDateField(BaseField):
    SERIALIZER = BrDateSerializer


class BinaryField(BaseField):
    SERIALIZER = rows.fields.BinaryField


class UUIDField(BaseField):
    SERIALIZER = rows.fields.UUIDField


class BoolField(BaseField):
    SERIALIZER = rows.fields.BoolField


class IntegerField(BaseField):
    SERIALIZER = rows.fields.IntegerField


class FloatField(BaseField):
    SERIALIZER = rows.fields.FloatField


class DecimalField(BaseField):
    SERIALIZER = rows.fields.DecimalField


class DateField(BaseField):
    SERIALIZER = rows.fields.DateField


class DatetimeField(BaseField):
    SERIALIZER = rows.fields.DatetimeField


class TextField(BaseField):
    SERIALIZER = rows.fields.TextField


class JSONField(BaseField):
    SERIALIZER = rows.fields.JSONField


class ChoiceField(BaseField):
    SERIALIZER = rows.fields.TextField
    VALUE_TYPE = str

    def __init__(self, choices, *args, **kwargs):
        self.choices = choices
        super().__init__(*args, **kwargs)

    @property
    def value(self):
        return super().value

    @value.setter
    def value(self, new_value):
        self.validate(new_value)
        options = list(self.choices.keys())
        if new_value not in options and new_value is None and not self.nullable:
            raise ValueError(
                f'Invalid choice option, for field "{self.name}": {new_value} is not in {options}')
        if new_value not in self.choices:
            choices_str = {f'{k} {k.__class__}': v for k, v in self.choices.items()}
            raise KeyError(f'{new_value} {new_value.__class__} for field "{self.name}" was not found on choices: {choices_str}')
        choice = self.choices[new_value]
        self._value = self.deserialize(choice)

    def check_values(self):
        super().check_values()
        for key, value in self.choices.items():
            self.validate(value)
            if not isinstance(value, self.VALUE_TYPE):
                if self.nullable and value is None:
                    continue
                raise ValueError(
                    f'Invalid value for field "{self.rel}": {value} is not {self.VALUE_TYPE}')


class IntegerChoiceField(ChoiceField):
    SERIALIZER = rows.fields.IntegerField
    VALUE_TYPE = int


class BoolChoiceField(ChoiceField):
    SERIALIZER = rows.fields.BoolField
    VALUE_TYPE = bool

package MR::Tarantool::Box::Record::Trait::Attribute::FieldObject;

use Mouse::Role;

has field => (
    is  => 'ro',
    isa => 'Str',
    required => 1,
);

has key => (
    is  => 'ro',
    isa => 'Str',
    required => 1,
);

has shard_by => (
    is  => 'ro',
    isa => 'Str | CodeRef',
);

has to_object => (
    is  => 'ro',
    isa => 'Str | CodeRef',
);

has selectors => (
    is  => 'ro',
    isa => 'HashRef[Str]',
    default => sub { {} },
);

before _process_options => sub {
    my ($class, $name, $args) = @_;
    if ($args->{to_object}) {
        confess "You can not use to_object and (lazy_build, default or builder) for the same attribute ($name)"
            if exists $args->{lazy_build} || exists $args->{default} || exists $args->{builder};
        $args->{lazy} = 1;
        $args->{default} = sub { confess "This subroutine should never be called" };
    }
    return;
};

before install_accessors => sub {
    my ($self) = @_;
    $self->apply_to_object();
    return;
};

after install_accessors => sub {
    my ($self) = @_;
    $self->modify_field();
    $self->install_selectors();
    return;
};

sub install_selectors {
    my ($self) = @_;
    my $associated_class = $self->associated_class;
    my $selectors = $self->selectors;
    foreach my $index (keys %$selectors) {
        my $method = $selectors->{$index};
        $associated_class->add_method($method => sub { shift->select($index, @_, by_object => 1) });
        $self->associate_method($method);
    }
    return;
}

sub modify_field {
    my ($self) = @_;
    my $associated_class = $self->associated_class;
    my $field = $associated_class->get_attribute($self->field) or return;
    $field->object($self->name) if $field->does('MR::Tarantool::Box::Record::Trait::Attribute::Field');
    my $reader = $self->get_read_method_ref();
    my $method = $self->key;
    my $object_name = $self->name;
    my $object_predicate = $self->{_mouse_cache_predicate_ref} ||= $self->_get_accessor_method_ref('predicate', '_generate_predicate');
    my $field_name = $field->name;
    my ($has_default, $default_value, $default_method);
    if ($has_default = $field->has_default || $field->has_builder) {
        if ($field->has_default) {
            if (ref $field->default eq 'CODE') {
                $default_method = $field->default;
            } else {
                $default_value = $field->default;
            }
        } else {
            $default_method = $field->builder;
        }
    }
    $associated_class->add_attribute('+' . $self->field,
        lazy    => 1,
        default => sub {
            if ($_[0]->$object_predicate) {
                my $value = $_[0]->$reader;
                return $value->$method;
            } elsif ($has_default) {
                return $default_method ? $_[0]->$default_method : $default_value;
            } else {
                confess "One of attributes '$object_name' or '$field_name' is required";
            }
        },
    );
    return;
}

sub apply_to_object {
    my ($self) = @_;
    my $to_object = $self->to_object or return;
    my $associated_class = $self->associated_class;
    my $field = $associated_class->get_attribute($self->field);
    my $object_name = $self->name;
    my $field_name = $field->name;
    my $reader = $field->get_read_method();
    $self->{default} = sub {
        local $_ = $_[0]->$reader;
        return $_[0]->$to_object($_);
    };
    return;
}

no Mouse::Role;

1;

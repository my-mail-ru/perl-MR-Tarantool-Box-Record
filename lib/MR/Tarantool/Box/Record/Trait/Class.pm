package MR::Tarantool::Box::Record::Trait::Class;

use Mouse::Role;
use MR::IProto::XS;
use MR::Tarantool::Box::XS;
use MR::Tarantool::Box::Record::Object;
use MR::Tarantool::Box::Record::Meta::Index;
use MR::Tarantool::Box::Record::Meta::Index::Part;
use Carp qw/cluck/;
use List::MoreUtils qw/uniq/;
use Scalar::Util qw/weaken/;
use POSIX qw/ceil/;

use Mouse::Util::TypeConstraints;

type 'MR::Tarantool::Box::Record::Trait::Class::Box' => where {
    my $class;
    if (ref $_) {
        $class = blessed $_ or return;
    } else {
        return unless Mouse::Util::is_class_loaded($_);
        $class = $_;
    }
    return $class->isa('MR::Tarantool::Box::XS');
};

type 'MR::Tarantool::Box::Record::Trait::Class::Box::Function' => where {
    my $class;
    if (ref $_) {
        $class = blessed $_ or return;
    } else {
        return unless Mouse::Util::is_class_loaded($_);
        $class = $_;
    }
    return $class->isa('MR::Tarantool::Box::XS::Function');
};

no Mouse::Util::TypeConstraints;

with 'MR::Tarantool::Box::Record::Trait::Class::FieldObject';
with 'MR::Tarantool::Box::Record::Trait::Class::Devel';

has iproto_class => (
    is  => 'rw',
    isa => 'ClassName',
    default => 'MR::IProto::XS',
);

has _iproto => (
    is  => 'rw',
    isa => 'MR::IProto::XS',
);

has space => (
    is  => 'rw',
    isa => 'Int',
);

has microshard_field => (
    is  => 'rw',
    isa => 'Str',
);

has microshard_bits => (
    is  => 'rw',
    isa => 'Int',
    default => 0,
);

has primary_key => (
    is  => 'ro',
    isa => 'MR::Tarantool::Box::Record::Meta::Index',
    lazy    => 1,
    default => sub {
        my ($self) = @_;
        foreach my $index (@{$self->indexes}) {
            return $index if $index->primary_key;
        }
        die sprintf "No primary_key defined for %s", $self->name;
    },
);

has override_by_lua => (
    is  => 'ro',
    isa => 'HashRef',
    default => sub { {} },
);

has box_class => (
    is  => 'rw',
    isa => 'ClassName',
    default => 'MR::Tarantool::Box::XS',
);

has box => (
    is  => 'rw',
    isa => 'MR::Tarantool::Box::Record::Trait::Class::Box',
    lazy    => 1,
    default => sub {
        my ($self) = @_;
        my (@fields, $format);
        foreach my $attr ($self->get_all_fields()) {
            push @fields, $attr->name;
            $format .= $attr->format;
        }
        my @indexes = map +{
            name    => $_->name,
            keys    => $_->fields,
            default => $_->default,
        }, @{$self->indexes};
        return $self->box_class->new(
            iproto    => $self->_iproto,
            namespace => $self->space,
            fields    => \@fields,
            format    => $format,
            indexes   => \@indexes,
        );
    },
);

has function_class => (
    is  => 'rw',
    isa => 'ClassName',
    default => 'MR::Tarantool::Box::XS::Function',
);

has insert_box => (
    is  => 'rw',
    isa => 'Maybe[MR::Tarantool::Box::Record::Trait::Class::Box::Function]',
    lazy    => 1,
    default => sub {
        my ($self) = @_;
        my $name = $self->override_by_lua->{insert} or return;
        my (@fields, $format);
        foreach my $attr ($self->get_all_fields()) {
            push @fields, $attr->name;
            $format .= $attr->format;
        }
        return $self->function_class->new(
            iproto     => $self->_iproto,
            name       => $name,
            in_fields  => \@fields,
            in_format  => $format,
            out_fields => \@fields,
            out_format => $format,
        );
    },
);

has delete_box => (
    is  => 'rw',
    isa => 'Maybe[MR::Tarantool::Box::Record::Trait::Class::Box::Function]',
    lazy    => 1,
    default => sub {
        my ($self) = @_;
        my $name = $self->override_by_lua->{delete} or return;
        my (@fields, $format);
        foreach my $attr ($self->get_all_fields()) {
            push @fields, $attr->name;
            $format .= $attr->format;
        }
        return $self->function_class->new(
            iproto     => $self->_iproto,
            name       => $name,
            in_fields  => $self->primary_key->fields,
            in_format  => join('', map $_->format, @{$self->primary_key->fields_attrs}),
            out_fields => \@fields,
            out_format => $format,
        );
    },
);

foreach my $attrname ('serialize', 'deserialize') {
    has $attrname => (
        is  => 'ro',
        isa => 'Maybe[CodeRef]',
        lazy    => 1,
        default => sub {
            my ($self) = @_;
            my %funcs;
            foreach my $attr ($self->get_all_fields()) {
                $funcs{$attr->name} = $attr->$attrname if $attr->$attrname;
            }
            return unless %funcs;
            return sub {
                my ($tuples) = @_;
                foreach my $tuple (@$tuples) {
                    while (my ($name, $func) = each %funcs) {
                        next unless exists $tuple->{$name};
                        local $_ = $tuple->{$name};
                        $tuple->{$name} = $func->($_, $tuple);
                    }
                }
                return;
            }
        },
    );
}

has validate => (
    is  => 'ro',
    isa => 'Maybe[CodeRef]',
    lazy    => 1,
    default => sub {
        my ($self) = @_;
        my $bits = $self->microshard_bits
            or return;
        my $mask = (1 << $bits) - 1;
        my $quarts = ceil($bits / 4);
        my (@int, @str);
        foreach my $field (grep $_->microsharding, $self->get_all_fields()) {
            if ($field->format eq '$' || $field->format eq '&') {
                push @str, $field->name;
            } else {
                push @int, $field->name;
            }
        }
        return unless @int || @str;
        return sub {
            my ($data, $shard_num) = @_;
            foreach my $f (@int) {
                confess "Value of field $f doesn't match shard_num ($data->{$f} not in $shard_num)"
                    unless ($data->{$f} & $mask) == $shard_num;
            }
            foreach my $f (@str) {
                confess "Value of field $f is too short" unless length $data->{$f} >= $quarts;
                my $v = reverse substr $data->{$f}, 0, $quarts;
                confess "Value of field $f doesn't match shard_num (\"$data->{$f}\" not in $shard_num)"
                    unless ($v & $mask) == $shard_num;
            }
            return;
        };
    },
);

has fields => (
    is  => 'ro',
    isa => 'ArrayRef[MR::Tarantool::Box::Record::Trait::Attribute::Field]',
    default => sub { [] },
);

has indexes => (
    is  => 'ro',
    isa => 'ArrayRef[MR::Tarantool::Box::Record::Meta::Index]',
    default => sub { [] },
);

has index_parts => (
    is  => 'ro',
    isa => 'ArrayRef[MR::Tarantool::Box::Record::Meta::Index::Part]',
    default => sub { [] },
);

has index_by_name => (
    is  => 'ro',
    isa => 'HashRef[MR::Tarantool::Box::Record::Meta::Index::Base]',
    lazy    => 1,
    default => sub {
        my ($self) = @_;
        my %data;
        foreach my $index (@{$self->indexes}, @{$self->index_parts}) {
            $data{$index->name} ||= $index;
            $data{join ',', @{$index->fields}} ||= $index;
        }
        return \%data;
    },
);

has fields_accessors => (
    is  => 'ro',
    isa => 'ArrayRef',
    lazy    => 1,
    default => sub { [ map uniq(grep defined, $_->get_read_method(), $_->get_write_method(), values %{$_->mutators}), $_[0]->get_all_fields() ] },
);

has sequence_iproto => (
    is  => 'rw',
    isa => 'MR::IProto::XS',
    lazy    => 1,
    default => sub { $_[0]->_iproto },
);

has sequence_namespace => (
    is  => 'rw',
    isa => 'Int',
    default => 0,
);

has select => (
    is  => 'ro',
    isa => 'CodeRef',
    init_arg => undef,
    lazy     => 1,
    default  => sub {
        my ($self) = @_;
        my %select_request;
        foreach my $index (@{$self->indexes}, @{$self->index_parts}) {
            $select_request{$index->name} ||= $index->select_request;
            $select_request{join ',', @{$index->fields}} ||= $index->select_request;
        }
        my $box = $self->box;
        my $select_response = $self->select_response;
        return sub {
            my ($class, $field, $keys, %opts) = @_;
            my %resp_opts = (
                objects             => $opts{objects} ||= {},
                create              => delete $opts{create},
                noraise_unuvailable => delete $opts{noraise_unuvailable},
            );
            my $select_request = $select_request{$field}
                or confess "Can't use field '$field' as an indexed field";
            (my $request, $resp_opts{bulk}) = $select_request->($keys, %opts);
            my $response = $box->bulk($request);
            return $class->$select_response($response, %resp_opts);
        }
    },
);

has select_response => (
    is  => 'ro',
    isa => 'CodeRef',
    init_arg => undef,
    lazy     => 1,
    default  => sub {
        my ($self) = @_;
        my $sharding = $self->box->iproto->get_shard_count() > 1;
        my $deserialize = $self->deserialize;
        my $object_tuples = $self->object_tuples;
        my %uniq_field_by_index = map { $_->uniq && @{$_->fields} == 1 ? ($_->index => $_->fields->[0]) : () } @{$self->indexes};
        weaken(my $meta = $self);
        return sub {
            my ($class, $response, %opts) = @_;

            my @list;
            my $created;
            foreach my $resp (@$response) {
                if ($resp->{error} == MR::Tarantool::Box::XS::ERR_CODE_OK) {
                    my $tuples = delete $resp->{tuples}; # "delete" is important. it prevents considirable performance penalty in perl 5.8 on "bless" bellow caused by S_reset_amagic
                    foreach my $tuple (@$tuples) {
                        $tuple->{shard_num} = $resp->{shard_num} if $sharding;
                        $tuple->{replica} = 1 if $resp->{replica};
                        $tuple->{exists} = 1;
                    }
                    if ($opts{create} && !$resp->{replica} && @$tuples != @{$resp->{keys}}) {
                        my $uniq_field = $uniq_field_by_index{$resp->{use_index}}
                            or confess "option 'create => 1' should be used only with unique singlefield indexes";
                        my %found = map { $_->{$uniq_field} => 1 } @$tuples;
                        my @created = map $class->_create_default($uniq_field => $_), grep !$found{$_}, @{$resp->{keys}};
                        push @list, @created;
                        $created = 1 if @created;
                    }
                    push @list, @$tuples;
                } elsif ($opts{noraise_unavailable}) {
                    my $count = @{$resp->{keys}};
                    cluck "Failed to select $class, $count items are unavailable: $resp->{error}";
                } else {
                    confess "Failed to select $class: $resp->{error}";
                }
            }
            if (@list) {
                $deserialize->(\@list) if $deserialize;
                if ($object_tuples) {
                    my $objects = $opts{objects};
                    $object_tuples->(\@list, $objects) if $objects && %$objects;
                }
                foreach (@list) {
                    bless $_, $class;
                    $meta->_initialize_object($_, {}, 1);
                }
                if ($created) {
                    $_->insert() foreach grep !$_->exists, @list;
                }
            }
            if ($opts{bulk}) {
                return \@list;
            } else {
                cluck sprintf "Select returned %d rows when only one row was expected", scalar @list if @list > 1;
                return $list[0];
            }
        }
    },
);

sub set_iproto {
    my $self = shift;
    my $iproto = @_ == 1 ? shift : $self->iproto_class->new(@_);
    $self->_iproto($iproto);
    return;
}

sub microshard {
    my ($self, $field, $bits) = @_;
    $self->microshard_field($field);
    $self->microshard_bits($bits);
    my $mask = (1 << $bits) - 1;
    $self->add_attribute('+shard_num',
        lazy    => 1,
        default => sub { $_[0]->$field & $mask },
    );
    return;
}

sub shard_by {
    my ($self, $code) = @_;
    $self->add_attribute('+shard_num',
        lazy    => 1,
        default => $code,
    );
    return;
}

sub add_field {
    my $self = shift;
    my $name = shift;
    my %args = @_ == 1 ? (format => shift) : @_;
    push @{$args{traits}}, 'MR::Tarantool::Box::Record::Trait::Attribute::Field';
    $self->add_attribute($name, is => 'rw', %args);
    return;
}

around add_attribute => sub {
    my ($orig, $self) = splice @_, 0, 2;
    my $attr = $self->$orig(@_);
    if ($attr->does('MR::Tarantool::Box::Record::Trait::Attribute::Field')) {
        my $number;
        my $fields = $self->fields;
        if (defined($number = $attr->number)) {
            confess "Field number $number already exists"
                if defined $fields->[$number] && $fields->[$number]->name ne $attr->name;
        } else {
            for ($number = 0; $number <= @$fields; $number++) {
                last unless defined $fields->[$number];
            }
            $attr->number($number);
        }
        $fields->[$number] = $attr;
    }
    return $attr;
};

sub add_index {
    my $self = shift;
    my $index;
    if (blessed $_[0]) {
        $index = shift;
        $index->associated_class($self);
    } else {
        my $name = shift;
        my %args = @_ == 0 ? (fields => $name) : @_ == 1 ? (fields => shift) : @_;
        $args{name} = $name unless ref $name;
        $args{fields} = [ $args{fields} ] unless ref $args{fields};
        $args{associated_class} = $self;
        $index = MR::Tarantool::Box::Record::Meta::Index->new(\%args);
    }
    $index->install_selectors();
    my $number;
    my $indexes = $self->indexes;
    if (defined($number = $index->number)) {
        confess "Index number $number already exists"
            if defined $indexes->[$number] && $indexes->[$number]->name ne $index->name;
    } else {
        for ($number = 1; $number <= @$indexes; $number++) {
            last unless defined $indexes->[$number];
        }
        $index->number($number);
    }
    $indexes->[$number] = $index;
    return;
}

sub add_index_part {
    my $self = shift;
    my $part;
    if (blessed $_[0]) {
        $part = shift;
        $part->associated_class($self);
    } else {
        my $name = shift;
        my %args = @_ == 1 ? (index => shift) : @_;
        $args{name} = $name;
        $args{associated_class} = $self;
        $part = MR::Tarantool::Box::Record::Meta::Index::Part->new(\%args);
    }
    $part->install_selectors();
    push @{$self->index_parts}, $part;
    return;
}

after make_immutable => sub {
    my ($self) = @_;
    $self->_validate_fields();
    $self->_validate_indexes();
    $self->primary_key;
    $self->box;
    $self->serialize;
    $self->deserialize;
    $self->index_by_name;
    $self->fields_accessors;
    $self->_install_select();
    return;
};

sub get_field_list {
    return map $_->name, @{$_[0]->fields};
}

sub get_all_fields {
    return @{$_[0]->fields};
}

sub initialize_sequence {
    my ($self, $field_name, $start_value) = @_;
    my $field = $self->get_attribute($field_name)
        or confess "Field $field_name not exists";
    my $sequence = $field->sequence
        or confess "Field $field_name has no sequence";
    $sequence->initialize_sequence($start_value);
    return;
}

sub _validate_fields {
    my ($self) = @_;
    my $fields = $self->fields;
    my @empty = grep !defined($fields->[$_]), (0 .. $#$fields);
    return unless @empty;
    my $numbers = join ', ', @empty;
    confess "Fields numbers $numbers are not defined";
}

sub _validate_indexes {
    my ($self) = @_;
    my $indexes = $self->indexes;
    my @empty = grep !defined($indexes->[$_]), (0 .. $#$indexes);
    return unless @empty;
    my $numbers = join ', ', @empty;
    confess "Indexes number $numbers are not defined";
}

sub _install_select {
    my ($self) = @_;
    if ($self->name->can('select') == \&MR::Tarantool::Box::Record::Object::select) {
        $self->add_method(select => $self->select);
    }
    return;
}

no Mouse::Role;

1;

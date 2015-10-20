package MR::Tarantool::Box::Record::Trait::Attribute::Field;

use Mouse::Util::TypeConstraints;

enum 'MR::Tarantool::Box::Record::Trait::Attribute::Field::Format' => qw( Q q L l S s C c & $ < > );
my %SIZEOF = (Q => 8, q => 8, L => 4, l => 4, S => 2, s => 2, C => 1, c => 1, '&' => undef, '$' => undef, '<' => undef, '>' => undef);

my $mutator_type = enum 'MR::Tarantool::Box::Record::Trait::Attribute::Field::Mutator' => qw( set inc dec add and xor or set_bit clear_bit );
subtype 'MR::Tarantool::Box::Record::Trait::Attribute::Field::MutatorHashRef' => as 'HashRef' => where {
    foreach (keys %$_) {
        return 0 unless $mutator_type->check($_);
    }
    return 1;
};

no Mouse::Util::TypeConstraints;

use Mouse::Role;
use MR::Tarantool::Box::Record::Meta::Index;
use MR::Tarantool::Box::Record::Meta::Sequence;
use Data::Dumper ();

with 'MR::Tarantool::Box::Record::Trait::Attribute::Devel';

has number => (
    is  => 'rw',
    isa => 'Int',
);

has format => (
    is  => 'ro',
    isa => 'MR::Tarantool::Box::Record::Trait::Attribute::Field::Format',
    default => '&',
);

has [qw/serialize deserialize/] => (
    is  => 'ro',
    isa => 'CodeRef',
);

has index => (
    is  => 'ro',
    isa => 'MR::Tarantool::Box::Record::Meta::Index',
);

has sequence => (
    is  => 'ro',
    isa => 'MR::Tarantool::Box::Record::Meta::Sequence',
);

has microsharding => (
    is  => 'ro',
    isa => 'Bool',
    lazy    => 1,
    default => sub { $_[0]->associated_class->microshard_bits && $_[0]->index ? 1 : 0 },
);

has mutators => (
    is  => 'rw',
    isa => 'MR::Tarantool::Box::Record::Trait::Attribute::Field::MutatorHashRef | ArrayRef[MR::Tarantool::Box::Record::Trait::Attribute::Field::Mutator]',
    default => sub { [] },
);

has object => (
    is  => 'rw',
    isa => 'Str',
);

has size => (
    is  => 'ro',
    isa => 'Maybe[Int]',
    lazy    => 1,
    default => sub { $SIZEOF{$_[0]->format} },
);

has min_size => (
    is  => 'ro',
    isa => 'Int',
    lazy    => 1,
    default => sub { $_[0]->size || 0 },
);

has max_size => (
    is  => 'ro',
    isa => 'Maybe[Int]',
    lazy    => 1,
    default => sub { $_[0]->size },
);

has ascii => (
    is  => 'ro',
    isa => 'Bool',
    lazy    => 1,
    default => 0,
);

{
    my %default = (
        map({ $_ => 0 } qw( Q q L l S s C c )),
        map({ $_ => "" } qw( & $ )),
    );

    before _process_options => sub {
        my ($class, $name, $args) = @_;

        $args->{required} = 1 if $args->{primary_key} && !exists $args->{required};

        my $trigger = sub { push @{$_[0]->_update_ops}, [ $name => set => $_[1] ] };
        if (my $orig_trigger = $args->{trigger}) {
            my $my_trigger = $trigger;
            $trigger = sub { $my_trigger->(@_); $orig_trigger->(@_) };
        }
        $args->{trigger} = $trigger;

        unless (blessed $args->{index}) {
            my %index_args = map { $_ => delete $args->{$_} } grep exists $args->{$_}, qw/primary_key uniq selector shard_by/;
            $index_args{name} = delete $args->{index} if exists $args->{index};
            $index_args{number} = delete $args->{index_number} if exists $args->{index_number};
            if (%index_args) {
                $index_args{fields} = [$name];
                $args->{index} = MR::Tarantool::Box::Record::Meta::Index->new(\%index_args);
            }
        }

        unless (blessed $args->{sequence}) {
            my %sequence_args = map { $_ => delete $args->{"sequence_$_"} } grep exists $args->{"sequence_$_"}, qw/id iproto namespace/;
            if (%sequence_args || delete $args->{sequence}) {
                $sequence_args{field} = $name;
                if (exists $sequence_args{namespace}) {
                    $sequence_args{space} = int($sequence_args{namespace});
                    delete $sequence_args{namespace};
                }
                $args->{sequence} = MR::Tarantool::Box::Record::Meta::Sequence->new(\%sequence_args);
            }
        }
        if ($args->{sequence} && !exists $args->{default} && !exists $args->{builder}) {
            $args->{lazy} = 1;
            $args->{builder} = $args->{sequence}->next_method;
        }

        if (!$args->{required} && !exists $args->{default} && !exists $args->{builder} && exists $default{$args->{format}}) {
            $args->{default} = $default{$args->{format}};
        }

        return;
    };
}

after install_accessors => sub {
    my ($self) = @_;
    if (my $index = $self->index) {
        $self->associated_class->add_index($index);
    }
    $self->_install_mutators();
    if (my $sequence = $self->sequence) {
        $sequence->associated_field($self);
        $sequence->install_methods();
    }
    return;
};

{
    my %op_alias = (
        inc       => 'num_add',
        dec       => 'num_sub',
        set_bit   => 'bit_set',
        clear_bit => 'bit_clear',
    );

    sub _install_mutators {
        my ($self) = @_;
        my $field = $self->name;
        my $associated_class = $self->associated_class;
        my %mutators = $self->_canonicalize_mutators();
        $self->mutators(\%mutators);
        foreach my $mutator (keys %mutators) {
            my $method = $mutators{$mutator};
            my $op = $op_alias{$mutator} || $mutator;
            my $sub = $mutator eq 'set' ? sub { $_[0]->$field($_[1]) }
                : $mutator eq 'add' ? sub {
                    $_[0]->$field($_[0]->$field + $_[1]);
                    $_[0]->_update_ops->[-1] = [ $field, $op, $_[1] ];
                    return;
                }
                : $mutator eq 'inc' ? sub {
                    my $v = @_ == 1 ? 1 : $_[1];
                    $_[0]->$field($_[0]->$field + $v);
                    $_[0]->_update_ops->[-1] = [ $field, $op, $v ];
                    return;
                }
                : $mutator eq 'dec' ? sub {
                    my $v = @_ == 1 ? 1 : $_[1];
                    $_[0]->$field($_[0]->$field - $v);
                    $_[0]->_update_ops->[-1] = [ $field, $op, $v ];
                    return;
                }
                : $mutator eq 'and' ? sub {
                    $_[0]->$field($_[0]->$field & $_[1]);
                    $_[0]->_update_ops->[-1] = [ $field, $op, $_[1] ];
                    return;
                }
                : $mutator eq 'set_bit' || $mutator eq 'or' ? sub {
                    $_[0]->$field($_[0]->$field | $_[1]);
                    $_[0]->_update_ops->[-1] = [ $field, $op, $_[1] ];
                    return;
                }
                : $mutator eq 'clear_bit' ? sub {
                    $_[0]->$field($_[0]->$field & ~$_[1]);
                    $_[0]->_update_ops->[-1] = [ $field, $op, $_[1] ];
                    return;
                }
                : $mutator eq 'xor' ? sub {
                    $_[0]->$field($_[0]->$field ^ $_[1]);
                    $_[0]->_update_ops->[-1] = [ $field, $op, $_[1] ];
                    return;
                }
                : confess "Unknown mutator: $mutator";
            $associated_class->add_method($method, $sub);
            $self->associate_method($method);
        }
        return;
    }
}

sub _canonicalize_mutators {
    my ($self) = @_;
    my $mutators = $self->mutators;
    if (ref $mutators eq 'ARRAY') {
        my $field = $self->name;
        return map { $_ => /^(\w+)_(\w+)$/ ? "$1_${field}_$2" : "${_}_${field}" } @$mutators;
    } else {
        return %$mutators;
    }
}

sub is_number {
    my ($self) = @_;
    return defined $SIZEOF{$self->format};
}

sub value_for_debug {
    my ($self, $value) = @_;
    return !defined $value ? 'undef' : $self->is_number() && $value =~ /^-?\d+$/ ? $value : Data::Dumper::qquote($value, $self->format =~ /^[\$<]$/ ? 'utf8' : undef);
}

no Mouse::Role;

1;

package MR::Tarantool::Box::Record::Trait::Attribute::Field;

use Mouse::Util::TypeConstraints;

enum 'MR::Tarantool::Box::Record::Trait::Attribute::Field::Format' => qw( Q q L l S s C c & $ );
my %SIZEOF = (Q => 8, q => 8, L => 4, l => 4, S => 2, s => 2, C => 1, c => 1, '&' => undef, '$' => undef);

my $mutator_type = enum 'MR::Tarantool::Box::Record::Trait::Attribute::Field::Mutator' => qw( set inc dec );
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

has primary_key => (
    is  => 'ro',
    isa => 'Bool',
);

has sequence => (
    is  => 'ro',
    isa => 'MR::Tarantool::Box::Record::Meta::Sequence',
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

{
    my %default = (
        map({ $_ => 0 } qw( Q q L l S s C c )),
        map({ $_ => "" } qw( & $ )),
    );

    before _process_options => sub {
        my ($class, $name, $args) = @_;

        $args->{required} = 1 if $args->{primary_key} && !exists $args->{required};

        my $trigger = sub { push @{$_[0]->_update_ops}, [ $name => set => $_[1] ] if $_[0]->_built };
        if (my $orig_trigger = $args->{trigger}) {
            my $my_trigger = $trigger;
            $trigger = sub { $my_trigger->(@_); $orig_trigger->(@_) };
        }
        $args->{trigger} = $trigger;

        unless (blessed $args->{index}) {
            my %index_args = map { $_ => delete $args->{$_} } grep exists $args->{$_}, qw/uniq selector shard_by/;
            $index_args{name} = delete $args->{index} if exists $args->{index};
            $index_args{default} = $args->{primary_key} if exists $args->{primary_key};
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
                $args->{sequence} = MR::Tarantool::Box::Record::Meta::Sequence->new(\%sequence_args);
            }
        }
        if ($args->{sequence} && !exists $args->{default} && !exists $args->{builder}) {
            $args->{lazy} = 1;
            $args->{builder} = $args->{sequence}->next_method;
        }

        if (!$args->{required} && !$args->{primary_key} && !exists $args->{default} && !exists $args->{builder} && exists $default{$args->{format}}) {
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
        inc => 'num_add',
        dec => 'num_sub',
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
            my $sub = $mutator eq 'inc' || $mutator eq 'dec' ? sub { push @{$_[0]->_update_ops}, [ $field, $op, @_ == 1 ? 1 : $_[1] ] }
                : sub { push @{$_[0]->_update_ops}, [ $field, $op, $_[1] ] };
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
        return map "${_}_${field}", @$mutators;
    } else {
        return %$mutators;
    }
}

no Mouse::Role;

1;

package MR::Tarantool::Box::Record::Trait::Attribute::Field;

use Mouse::Util::TypeConstraints;

enum 'MR::Tarantool::Box::Record::Trait::Attribute::Field::Format' => qw( L l S s C c & $ );
enum 'MR::Tarantool::Box::Record::Trait::Attribute::Field::Mutator' => qw( set inc dec );

no Mouse::Util::TypeConstraints;

use Mouse::Role;

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
    isa => 'Str',
);

has selector => (
    is  => 'ro',
    isa => 'Str',
);

has primary_key => (
    is  => 'ro',
    isa => 'Bool',
);

has mutators => (
    is  => 'ro',
    isa => 'ArrayRef[MR::Tarantool::Box::Record::Trait::Attribute::Field::Mutator]',
    default => sub { [] },
);

{
    my %default = (
        map({ $_ => 0 } qw( L l S s C c )),
        map({ $_ => "" } qw( & $ )),
    );

    before _process_options => sub {
        my ($class, $name, $args) = @_;

        $args->{required} = 1 if $args->{primary_key};

        $args->{default} = $default{$args->{format}}
            if !exists $args->{default} && exists $default{$args->{format}};

        my $trigger = sub { push @{$_[0]->_update_ops}, [ $name => set => $_[1] ] if $_[0]->_built };
        if (my $orig_trigger = $args->{trigger}) {
            my $my_trigger = $trigger;
            $trigger = sub { $my_trigger->(@_); $orig_trigger->(@_) };
        }
        $args->{trigger} = $trigger;

        return;
    };
}

{
    my %op_alias = (
        inc => 'num_add',
        dec => 'num_sub',
    );

    after install_accessors => sub {
        my ($self) = @_;
        my $associated_class = $self->associated_class;
        if (my $selector = $self->selector) {
            my $field = $self->name;
            $self->index("idx_$field") unless $self->index;
            $associated_class->add_method($selector => sub {
                my ($self, $keys) = @_;
                return $self->select($field => $keys);
            });
            $self->associate_method($selector);
        }
        foreach my $mutator (@{$self->mutators}) {
            my $field = $self->name;
            my $name = $mutator . '_' . $field;
            my $op = $op_alias{$mutator} || $mutator;
            my $sub = $mutator eq 'inc' || $mutator eq 'dec' ? sub { push @{$_[0]->_update_ops}, [ $field, $op, @_ == 1 ? 1 : $_[1] ] }
                : sub { push @{$_[0]->_update_ops}, [ $field, $op, $_[1] ] };
            $associated_class->add_method($name, $sub);
            $self->associate_method($name);
        }
        return;
    };
}

no Mouse::Role;

1;

package MR::Tarantool::Box::Record::Meta::Index;

use Mouse;

has associated_class => (
    is  => 'rw',
    isa => 'MR::Tarantool::Box::Record::Trait::Class',
    weak_ref => 1,
);

has name => (
    is  => 'ro',
    isa => 'Str',
    lazy    => 1,
    default => sub { join ',', map $_->name, @{$_[0]->fields} },
);

has fields => (
    is  => 'ro',
    isa => 'ArrayRef[Str]',
    required => 1,
);

has default => (
    is  => 'ro',
    isa => 'Bool',
);

has uniq => (
    is  => 'ro',
    isa => 'Bool',
    default => 1,
);

has multifield => (
    is  => 'ro',
    isa => 'Bool',
    lazy    => 1,
    default => sub { @{$_[0]->fields} > 1 },
);

has selector => (
    is  => 'ro',
    isa => 'Str',
);

has prepare_keys => (
    is  => 'ro',
    isa => 'Maybe[CodeRef]',
    lazy    => 1,
    default => sub {
        my ($self) = @_;
        my $associated_class = $self->associated_class;
        my @attrs = map $associated_class->get_attribute($_), @{$self->fields};
        my @name = map $_->name, @attrs;
        my @type = map $_->type_constraint, @attrs;
        my @serialize = map $_->serialize, @attrs;
        return unless @type || @serialize;
        return sub {
            my ($keys) = @_;
            if (@type) {
                foreach my $key (@$keys) {
                    if (ref $key) {
                        for (0 .. $#$key) {
                            cluck(sprintf "%s doesn't look like %s", defined $key->[$_] ? "'$key->[$_]'" : 'undef', $name[$_]) if $type[$_] && !$type[$_]->check($key->[$_]);
                        }
                    } else {
                        cluck(sprintf "%s doesn't look like %s", defined $key ? "'$key'" : 'undef', $name[0]) if $type[0] && !$type[0]->check($key);
                    }
                }
            }
            if (@serialize) {
                $keys = [
                    map {
                        ref $_
                            ? do { my $f = $_; [ map { $serialize[$_] ? $serialize[$_]->($f->[$_]) : $f->[$_] } (0 .. $#$f) ] }
                            : $serialize[0] ? $serialize[0]->($_) : $_
                    } @$keys
                ];
            }
            return $keys;
        };
    },
);

sub install_selectors {
    my ($self) = @_;
    my $name = $self->name;
    my $associated_class = $self->associated_class;
    if (my $selector = $self->selector) {
        $associated_class->add_method($selector => sub { shift->select($name, @_) });
        $self->associate_method($selector);
    }
    return;
}

sub associate_method {
    my ($self, $method_name) = @_;
    return;
}

no Mouse;
__PACKAGE__->meta->make_immutable();

1;

package MR::Tarantool::Box::Record::Meta::Index::Base;

use Mouse;
use Carp qw/cluck/;

with 'MR::Tarantool::Box::Record::Trait::Index::FieldObject';

has associated_class => (
    is  => 'rw',
    isa => 'MR::Tarantool::Box::Record::Trait::Class',
    weak_ref => 1,
);

has name => (
    is  => 'ro',
    isa => 'Str',
    lazy    => 1,
    default => sub { join ',', @{$_[0]->fields} },
);

has index => (
    is  => 'ro',
    isa => 'Str',
    lazy    => 1,
    default => sub { $_[0]->name },
);

has fields => (
    is  => 'ro',
    isa => 'ArrayRef[Str]',
);

has uniq => (
    is  => 'ro',
    isa => 'Bool',
    default => 1,
);

has default_limit => (
    is  => 'ro',
    isa => 'Maybe[Int]',
    predicate => 'has_default_limit',
);

has selector => (
    is  => 'ro',
    isa => 'Str',
);

has multifield => (
    is  => 'ro',
    isa => 'Bool',
    init_arg => undef,
    lazy     => 1,
    default  => sub { @{$_[0]->fields} > 1 },
);

has 'shard_by' => (
    is  => 'ro',
    isa => 'Maybe[CodeRef|Str]',
);

has fields_attrs => (
    is  => 'ro',
    isa => 'ArrayRef',
    init_arg => undef,
    lazy     => 1,
    default  => sub { [ map $_[0]->associated_class->get_attribute($_), @{$_[0]->fields} ] },
);

has prepare_keys => (
    is  => 'ro',
    isa => 'Maybe[CodeRef]',
    init_arg => undef,
    lazy     => 1,
    default  => sub {
        my ($self) = @_;
        my $attrs = $self->fields_attrs;
        my @name = map $_->name, @$attrs;
        my @type = map $_->type_constraint, @$attrs;
        my @serialize = map $_->serialize, @$attrs;
        return unless grep $_, @type, @serialize;
        return sub {
            my ($keys) = @_;
            if (@type) {
                foreach my $key (@$keys) {
                    if (ref $key eq 'ARRAY') {
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
                        ref $_ eq 'ARRAY'
                            ? do { my $f = $_; [ map { my $s = $serialize[$_]; $s ? do { local $_ = $f->[$_]; $s->($_) } : $f->[$_] } (0 .. $#$f) ] }
                            : $serialize[0] ? $serialize[0]->($_) : $_
                    } @$keys
                ];
            }
            return $keys;
        };
    },
);

has prepare_key => (
    is  => 'ro',
    isa => 'CodeRef',
    init_arg => undef,
    lazy     => 1,
    default  => sub {
        my ($self) = @_;
        my $attrs = $self->fields_attrs;
        my @name = map $_->name, @$attrs;
        my @serialize = map $_->serialize, @$attrs;
        if (grep $_, @serialize) {
            return sub {
                my ($record) = @_;
                return { map { my $n = $name[$_]; my $s = $serialize[$_]; my $v = $record->$n; ($n => $s ? do { local $_ = $v; $s->($_) } : $v) } (0 .. $#name) };
            };
        } else {
            return sub {
                my ($record) = @_;
                return { map { $_ => $record->$_ } @name };
            };
        }
    },
);

has select_request => (
    is  => 'ro',
    isa => 'CodeRef',
    init_arg => undef,
    lazy     => 1,
    default  => sub {
        my ($self) = @_;
        my $index_no = $self->index;
        my $uniq = $self->uniq;
        my $multifield = @{$self->fields} > 1;
        my $uniq_field = $uniq && !$multifield ? $self->fields->[0] : undef;
        my $has_default_limit = $self->has_default_limit();
        my $default_limit = $self->default_limit;
        my $deobject_keys = $self->deobject_keys;
        my $prepare_keys = $self->prepare_keys;
        my $shard_count = $self->associated_class->box->iproto->get_shard_count();
        my $shard_by = $self->shard_by;
        my $class = $self->associated_class->name;
        return sub {
            my ($keys, %opts) = @_;

            my $objects = delete $opts{objects};
            my $by_object = delete $opts{by_object};

            my $keys_is_bulk = $multifield ? ref $keys eq 'HASH' || ref $keys eq 'ARRAY' && (!@$keys || ref $keys->[0] eq 'ARRAY')
                : ref $keys && (
                    ref $keys eq 'HASH' || ref $keys eq 'ARRAY'
                    || $by_object && blessed $keys && overload::Method($keys, '@{}')
                );
            $keys = [ $keys ] unless $keys_is_bulk;
            my $bulk = $keys_is_bulk || !$uniq;

            $opts{inplace} = 1;
            $opts{type} = 'select';
            $opts{use_index} = $index_no;
            $opts{limit} = $default_limit if $has_default_limit && !exists $opts{limit};
            confess "option 'limit' should be specified or 'default_limit' should be set on index if non-unique index is used" if !$uniq && !exists $opts{limit};

            if ($by_object && $deobject_keys) {
                $keys = $deobject_keys->($keys, $objects);
            }
            my $shard_keys;
            if ($shard_count == 1) {
                confess "\$keys shouldn't be passed as HASHREF if sharding is not used" if ref $keys eq 'HASH';
                $keys = $prepare_keys->($keys) if $prepare_keys;
            } elsif (ref $keys eq 'HASH') {
                confess "option 'shard_num' shouldn't be used with \$keys passed as a HASHREF" if exists $opts{shard_num};
                $shard_keys = $prepare_keys ? { map { $_ => $prepare_keys->($keys->{$_}) } keys %$keys } : $keys;
            } elsif (exists $opts{shard_num}) {
                $keys = $prepare_keys->($keys) if $prepare_keys;
                if ($opts{shard_num} eq 'all') {
                    delete $opts{shard_num};
                    $shard_keys = { map { $_ => $keys } (1 .. $shard_count) };
                }
            } elsif ($shard_by) {
                my $shard_nums;
                my $shard_num = $class->$shard_by($keys, objects => $objects);
                $keys = $prepare_keys->($keys) if $prepare_keys;
                if (ref $shard_num) {
                    confess "size of ARRAYREF returned by 'shard_by' function should be equal to size of keys ARRAYREF" unless @$shard_num == @$keys;
                    $shard_keys = {};
                    foreach (0 .. $#$keys) {
                        my $sn = $shard_num->[$_] or next;
                        push @{$shard_keys->{$sn}}, $keys->[$_];
                    }
                } elsif ($shard_num eq 'all') {
                    $shard_keys = { map { $_ => $keys } (1 .. $shard_count) };
                } else {
                    $opts{shard_num} = $shard_num;
                }
            }

            my $single = delete $opts{single};
            my @request;
            if ($shard_keys) {
                @request = map +{ %opts, keys => $shard_keys->{$_}, shard_num => $_ }, keys %$shard_keys;
            } else {
                $opts{keys} = $keys;
                @request = (\%opts);
            }
            @request = map { my $r = $_; map +{ %$r, keys => [ $_ ] }, @{$r->{keys}} } @request if $single;
            return \@request, $bulk;
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

sub key_for_debug {
    my ($self, $key) = @_;
    $key = [ $key ] unless ref $key;
    my $hash = ref $key eq 'HASH';
    return sprintf '[ %s ]', join ', ', map {
        my $value = $hash ? $key->{$_->name} : $key->[$_->number];
        sprintf "%s: %s", $_->name, $_->value_for_debug($value);
    } @{$self->fields_attrs};
}

no Mouse;
__PACKAGE__->meta->make_immutable();

1;

package MR::Tarantool::Box::Record::Trait::Index::FieldObject;

use Mouse::Role;

has deobject_keys => (
    is  => 'ro',
    isa => 'Maybe[CodeRef]',
    init_arg => undef,
    lazy     => 1,
    default  => sub {
        my ($self) = @_;
        my $attrs = $self->fields_attrs;
        my @fields = map $_->name, @$attrs;
        my @object = map $_->object, @$attrs;
        return unless grep $_, @object;
        my (@methods, @shard_methods);
        my $associated_class = $self->associated_class;
        foreach my $i (0 .. $#object) {
            next unless defined $object[$i];
            my $obj = $associated_class->get_attribute($object[$i]);
            $methods[$i] = $obj->key;
            $shard_methods[$i] = $obj->shard_by;
        }
        my $can_shard = grep $_, @shard_methods;
        return sub {
            my ($keys) = @_;
            my %map;
            if (ref $keys eq 'HASH') {
                my %keys;
                foreach my $shard_num (keys %$keys) {
                    my @keys;
                    foreach my $key (@{$keys->{$shard_num}}) {
                        if (ref $key eq 'ARRAY') {
                            my @key;
                            foreach my $i (0 .. $#$key) {
                                if (my $method = $methods[$i]) {
                                    $key[$i] = $key->[$i]->$method;
                                    $map{$fields[$i]}{$key[$i]} = $key->[$i];
                                } else {
                                    $key[$i] = $key->[$i];
                                }
                            }
                            push @keys, \@key;
                        } else {
                            my $method = $methods[0];
                            my $res = $key->$method;
                            $map{$fields[0]}{$res} = $key;
                            push @keys, $res;
                        }
                    }
                    $keys{$shard_num} = \@keys;
                }
                return (\%keys, \%map);
            } elsif ($can_shard) {
                my %keys;
                foreach my $key (@$keys) {
                    if (ref $key eq 'ARRAY') {
                        my (@key, $shard_num);
                        foreach my $i (0 .. $#$key) {
                            if (my $method = $methods[$i]) {
                                $key[$i] = $key->[$i]->$method;
                                $map{$fields[$i]}{$key[$i]} = $key->[$i];
                                if (my $shard_method = $shard_methods[$i]) {
                                    $shard_num ||= $key->[$i]->$shard_method;
                                }
                            } else {
                                $key[$i] = $key->[$i];
                            }
                        }
                        push @{$keys{$shard_num}}, \@key;
                    } else {
                        my $method = $methods[0];
                        my $shard_method = $shard_methods[0];
                        my $res = $key->$method;
                        $map{$fields[0]}{$res} = $key;
                        push @{$keys{$key->$shard_method}}, $res;
                    }
                }
                return (\%keys, \%map);
            } else {
                my @keys;
                foreach my $key (@$keys) {
                    if (ref $key eq 'ARRAY') {
                        my @key;
                        foreach my $i (0 .. $#$key) {
                            if (my $method = $methods[$i]) {
                                $key[$i] = $key->[$i]->$method;
                                $map{$fields[$i]}{$key[$i]} = $key->[$i];
                            } else {
                                $key[$i] = $key->[$i];
                            }
                        }
                        push @keys, \@key;
                    } else {
                        my $method = $methods[0];
                        my $res = $key->$method;
                        $map{$fields[0]}{$res} = $key;
                        push @keys, $res;
                    }
                }
                return (\@keys, \%map);
            }
        };
    },
);

has object_tuples => (
    is  => 'ro',
    isa => 'Maybe[CodeRef]',
    init_arg => undef,
    lazy     => 1,
    default  => sub {
        my ($self) = @_;
        my $attrs = $self->fields_attrs;
        my %object = map { $_->name => $_->object } grep $_->object, @$attrs;
        return unless %object;
        return sub {
            my ($tuples, $map) = @_;
            foreach my $tuple (@$tuples) {
                foreach my $field (keys %$map) {
                    next unless exists $tuple->{$field};
                    $tuple->{$object{$field}} = $map->{$field}->{$tuple->{$field}};
                }
            }
            return;
        }
    }
);

no Mouse::Role;

1;

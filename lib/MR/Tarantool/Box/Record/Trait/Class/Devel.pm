package MR::Tarantool::Box::Record::Trait::Class::Devel;

use Mouse::Role;

sub approximate_storage_size {
    my ($self) = @_;
    my $fragmentation = 0.2;
    my $index_size = 0;
    foreach my $index (@{$self->indexes}) {
        $index_size += $index->storage_size;
    }
    my ($min, $max) = (0, 0);
    foreach my $field ($self->get_all_fields()) {
        $min += $field->min_storage_size;
        if (defined $max && defined $field->max_storage_size) {
            $max += $field->max_storage_size;
        } else {
            $max = undef;
        }
    }
    return ($min / (1 - $fragmentation) + $index_size, $max / (1 - $fragmentation) + $index_size);
}

sub print_approximate_storage_size {
    my ($self, $count) = @_;
    $count = 1 unless defined $count;
    my ($min, $max) = $self->approximate_storage_size();
    $min = sprintf "%.02f Gb", $min * $count / (1024 * 1024 * 1024);
    $max = defined $max ? sprintf "%.02f Gb", $max * $count / (1024 * 1024 * 1024) : 'inf';
    print "$min .. $max\n";
    return;
}

sub print_storage_config {
    my ($self) = @_;
    printf "# %s\n", $self->name;
    my $namespace = $self->namespace;
    printf "object_space[%s].enabled = 1\n", $namespace;
    foreach my $index (@{$self->indexes}) {
        printf "object_space[%s].index[%d].type = \"%s\"\n", $namespace, $index->number, $index->type;
        printf "object_space[%s].index[%d].unique = %d\n", $namespace, $index->number, $index->uniq ? 1 : 0;
        my $i = 0;
        foreach my $field (map $self->get_attribute($_), @{$index->fields}) {
            printf "object_space[%s].index[%d].key_field[%d].fieldno = %s\n", $namespace, $index->number, $i, $field->number;
            printf "object_space[%s].index[%d].key_field[%d].type = \"%s\"\n", $namespace, $index->number, $i, $index->index_field_type($field);
            $i++;
        }
    }
}

no Mouse::Role;

1;

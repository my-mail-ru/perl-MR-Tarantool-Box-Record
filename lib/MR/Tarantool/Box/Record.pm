package MR::Tarantool::Box::Record;

use Mouse qw/confess/;
use Mouse::Exporter;
use Mouse::Util::MetaRole;
use MR::Tarantool::Box::Record::Object;
use MR::Tarantool::Box::Record::Trait::Class;

Mouse::Exporter->setup_import_methods(
    as_is => [ 'iproto', 'namespace', 'microshard', 'shard_by', 'override_by_lua', 'has_field', 'has_field_object', 'has_index', 'has_index_part' ],
    also  => 'Mouse',
);

sub init_meta {
    my ($class, %args) = @_;
    Mouse->init_meta(base_class => 'MR::Tarantool::Box::Record::Object', %args);
    Mouse::Util::MetaRole::apply_metaroles(
        for => $args{for_class},
        class_metaroles => {
            class => ['MR::Tarantool::Box::Record::Trait::Class'],
        },
    );
    return $args{for_class}->meta();
}

sub iproto {
    caller->meta->set_iproto(@_);
}

sub namespace {
    caller->meta->namespace(@_);
}

sub microshard ($$) {
    caller->meta->microshard(@_);
}

sub shard_by (&) {
    caller->meta->shard_by(@_);
}

sub override_by_lua ($$) {
    confess "Only 'insert' and 'delete' operations can be overrided" unless $_[0] eq 'insert' || $_[0] eq 'delete';
    caller->meta->override_by_lua->{$_[0]} = $_[1];
}

sub has_field {
    caller->meta->add_field(@_);
}

sub has_field_object {
    caller->meta->add_field_object(@_);
}

sub has_index {
    caller->meta->add_index(@_);
}

sub has_index_part {
    caller->meta->add_index_part(@_);
}

1;

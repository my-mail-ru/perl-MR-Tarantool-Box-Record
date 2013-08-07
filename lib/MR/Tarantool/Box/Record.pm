package MR::Tarantool::Box::Record;

use Mouse ();
use Mouse::Exporter;
use Mouse::Util::MetaRole;
use MR::Tarantool::Box::Record::Object;
use MR::Tarantool::Box::Record::Trait::Class;

Mouse::Exporter->setup_import_methods(
    as_is => [ 'iproto', 'namespace', 'shard_by', 'has_field', 'has_field_object', 'has_index', 'has_index_part' ],
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

sub shard_by (&) {
    caller->meta->shard_by(@_);
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

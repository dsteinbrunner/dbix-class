package DBIx::Class::SQLMaker;

use strict;
use warnings;

=head1 NAME

DBIx::Class::SQLMaker - An SQL::Abstract-based SQL maker class

=head1 DESCRIPTION

This module is a subclass of L<SQL::Abstract> and includes a number of
DBIC-specific workarounds, not yet suitable for inclusion into the
L<SQL::Abstract> core. It also provides all (and more than) the functionality
of L<SQL::Abstract::Limit>, see L<DBIx::Class::SQLMaker::LimitDialects> for
more info.

Currently the enhancements to L<SQL::Abstract> are:

=over

=item * Support for C<JOIN> statements (via extended C<table/from> support)

=item * Support of functions in C<SELECT> lists

=item * C<GROUP BY>/C<HAVING> support (via extensions to the order_by parameter)

=item * Support of C<...FOR UPDATE> type of select statement modifiers

=back

=cut

use base qw/
  SQL::Abstract
  DBIx::Class::SQLMaker::LimitDialects
/;
use mro 'c3';

use Module::Runtime qw(use_module);
use Sub::Name 'subname';
use DBIx::Class::Carp;
use DBIx::Class::Exception;
use Moo;
use namespace::clean;

has limit_dialect => (
  is => 'rw', default => sub { 'LimitOffset' },
  trigger => sub { shift->clear_renderer_class }
);

around _build_renderer_roles => sub {
  my ($orig, $self) = (shift, shift);
  return (
    $self->$orig(@_),
    'Data::Query::Renderer::SQL::Slice::'.$self->limit_dialect
  );
};

# for when I need a normalized l/r pair
sub _quote_chars {
  map
    { defined $_ ? $_ : '' }
    ( ref $_[0]->{quote_char} ? (@{$_[0]->{quote_char}}) : ( ($_[0]->{quote_char}) x 2 ) )
  ;
}

sub _build_converter_class {
  Module::Runtime::use_module('DBIx::Class::SQLMaker::Converter')
}

# FIXME when we bring in the storage weaklink, check its schema
# weaklink and channel through $schema->throw_exception
sub throw_exception { DBIx::Class::Exception->throw($_[1]) }

BEGIN {
  # reinstall the belch()/puke() functions of SQL::Abstract with custom versions
  # that use DBIx::Class::Carp/DBIx::Class::Exception instead of plain Carp
  no warnings qw/redefine/;

  *SQL::Abstract::belch = subname 'SQL::Abstract::belch' => sub (@) {
    my($func) = (caller(1))[3];
    carp "[$func] Warning: ", @_;
  };

  *SQL::Abstract::puke = subname 'SQL::Abstract::puke' => sub (@) {
    my($func) = (caller(1))[3];
    __PACKAGE__->throw_exception("[$func] Fatal: " . join ('',  @_));
  };
}

# the "oh noes offset/top without limit" constant
# limited to 31 bits for sanity (and consistency,
# since it may be handed to the like of sprintf %u)
#
# Also *some* builds of SQLite fail the test
#   some_column BETWEEN ? AND ?: 1, 4294967295
# with the proper integer bind attrs
#
# Implemented as a method, since ::Storage::DBI also
# refers to it (i.e. for the case of software_limit or
# as the value to abuse with MSSQL ordered subqueries)
sub __max_int () { 0x7FFFFFFF };

# poor man's de-qualifier
sub _quote {
  $_[0]->next::method( ( $_[0]{_dequalify_idents} and ! ref $_[1] )
    ? $_[1] =~ / ([^\.]+) $ /x
    : $_[1]
  );
}

sub _where_op_NEST {
  carp_unique ("-nest in search conditions is deprecated, you most probably wanted:\n"
      .q|{..., -and => [ \%cond0, \@cond1, \'cond2', \[ 'cond3', [ col => bind ] ], etc. ], ... }|
  );

  shift->next::method(@_);
}

# Handle limit-dialect selection
sub select {
  my ($self, $table, $fields, $where, $rs_attrs, $limit, $offset) = @_;

  if (defined $offset) {
    $self->throw_exception('A supplied offset must be a non-negative integer')
      if ( $offset =~ /\D/ or $offset < 0 );
  }
  $offset ||= 0;

  if (defined $limit) {
    $self->throw_exception('A supplied limit must be a positive integer')
      if ( $limit =~ /\D/ or $limit <= 0 );
  }
  elsif ($offset) {
    $limit = $self->__max_int;
  }

  my %final_attrs = (%{$rs_attrs||{}}, limit => $limit, offset => $offset);

  my %slice_stability = $self->renderer->slice_stability;

  if (my $stability = $slice_stability{$offset ? 'offset' : 'limit'}) {
    my $source = $rs_attrs->{_rsroot_rsrc};
    unless (
      $final_attrs{order_is_stable}
      = $final_attrs{preserve_order}
      = $source->schema->storage
               ->_order_by_is_stable(
                   @final_attrs{qw(from order_by where)}
                 )
    ) {
      if ($stability eq 'requires') {
        if ($self->converter->_order_by_to_dq($final_attrs{order_by})) {
          $self->throw_exception(
            $self->limit_dialect.' limit/offset implementation requires a stable order for offset'
          );
        }
        if (my $ident_cols = $source->_identifying_column_set) {
          $final_attrs{order_by} = [
            map "$final_attrs{alias}.$_", @$ident_cols
          ];
          $final_attrs{order_is_stable} = 1;
        } else {
          $self->throw_exception(sprintf(
            'Unable to auto-construct stable order criteria for "skimming type" 
limit '
          . "dialect based on source '%s'", $source->name) );
        }
      }
    }

  }

  my %slice_subquery = $self->renderer->slice_subquery;

  if (my $subquery = $slice_subquery{$offset ? 'offset' : 'limit'}) {
    $fields = [ map {
      my $f = $fields->[$_];
      if (ref $f) {
        $f = { '' => $f } unless ref($f) eq 'HASH';
        $f->{-as} ||= $final_attrs{as}[$_];
      }
      $f;
    } 0 .. $#$fields ];
  }

  my ($sql, @bind) = $self->next::method ($table, $fields, $where, $final_attrs{order_by}, \%final_attrs );

  $sql .= $self->_lock_select ($rs_attrs->{for})
    if $rs_attrs->{for};

  return wantarray ? ($sql, @bind) : $sql;
}

sub _assemble_binds {
  my $self = shift;
  return map { @{ (delete $self->{"${_}_bind"}) || [] } } (qw/pre_select select from where group having order limit/);
}

my $for_syntax = {
  update => 'FOR UPDATE',
  shared => 'FOR SHARE',
};
sub _lock_select {
  my ($self, $type) = @_;

  my $sql;
  if (ref($type) eq 'SCALAR') {
    $sql = "FOR $$type";
  }
  else {
    $sql = $for_syntax->{$type} || $self->throw_exception( "Unknown SELECT .. FOR type '$type' requested" );
  }

  return " $sql";
}

sub _split_order_chunk {
  my ($self, $chunk) = @_;

  # strip off sort modifiers, but always succeed, so $1 gets reset
  $chunk =~ s/ (?: \s+ (ASC|DESC) )? \s* $//ix;

  return (
    $chunk,
    ( $1 and uc($1) eq 'DESC' ) ? 1 : 0,
  );
}

sub _recurse_from {
  scalar shift->_render_sqla(table => \@_);
}

# This is hideously ugly, but SQLA does not understand multicol IN expressions
# FIXME TEMPORARY - DQ should have native syntax for this
# moved here to raise API questions
#
# !!! EXPERIMENTAL API !!! WILL CHANGE !!!
sub _where_op_multicolumn_in {
  my ($self, $lhs, $rhs) = @_;

  if (! ref $lhs or ref $lhs eq 'ARRAY') {
    my (@sql, @bind);
    for (ref $lhs ? @$lhs : $lhs) {
      if (! ref $_) {
        push @sql, $self->_quote($_);
      }
      elsif (ref $_ eq 'SCALAR') {
        push @sql, $$_;
      }
      elsif (ref $_ eq 'REF' and ref $$_ eq 'ARRAY') {
        my ($s, @b) = @$$_;
        push @sql, $s;
        push @bind, @b;
      }
      else {
        $self->throw_exception("ARRAY of @{[ ref $_ ]}es unsupported for multicolumn IN lhs...");
      }
    }
    $lhs = \[ join(', ', @sql), @bind];
  }
  elsif (ref $lhs eq 'SCALAR') {
    $lhs = \[ $$lhs ];
  }
  elsif (ref $lhs eq 'REF' and ref $$lhs eq 'ARRAY' ) {
    # noop
  }
  else {
    $self->throw_exception( ref($lhs) . "es unsupported for multicolumn IN lhs...");
  }

  # is this proper...?
  $rhs = \[ $self->_recurse_where($rhs) ];

  for ($lhs, $rhs) {
    $$_->[0] = "( $$_->[0] )"
      unless $$_->[0] =~ /^ \s* \( .* \) \s* ^/xs;
  }

  \[ join( ' IN ', shift @$$lhs, shift @$$rhs ), @$$lhs, @$$rhs ];
}

1;

=head1 AUTHORS

See L<DBIx::Class/CONTRIBUTORS>.

=head1 LICENSE

You may distribute this code under the same terms as Perl itself.

=cut

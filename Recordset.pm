
###################################################################################
#
#   DBIx::Recordset - Copyright (c) 1997-1998 Gerald Richter / ECOS
#
#   You may distribute under the terms of either the GNU General Public
#   License or the Artistic License, as specified in the Perl README file.
#   For use with Apache httpd and mod_perl, see also Apache copyright.
#
#   THIS IS BETA SOFTWARE!
#
#   THIS PACKAGE IS PROVIDED "AS IS" AND WITHOUT ANY EXPRESS OR
#   IMPLIED WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED
#   WARRANTIES OF MERCHANTIBILITY AND FITNESS FOR A PARTICULAR PURPOSE.
#
###################################################################################


package DBIx::Recordset ;

use strict 'vars' ;
use Carp ;

use DBIx::Compat ;

#use Devel::Peek ;

use vars 
    qw(
    $VERSION
    @ISA
    @EXPORT
    @EXPORT_OK

    $self
    @self
    %self
    
    $newself

    $Debug 

    $fld
    @fld 

    %Compat

    $id
    $numOpen

    %Data
    %Metadata
    );

use DBI ;

require Exporter;

@ISA       = qw(Exporter);

$VERSION = '0.18-beta';


$id = 1 ;
$numOpen = 0 ;

$Debug = 0 ;     # Disable debugging output

# Get filehandle of logfile
if (defined ($INC{'HTML/Embperl.pm'}))
    {
    tie *LOG, 'HTML::Embperl::Log' ;
    }
else
    {
    *LOG = \*STDOUT ; 
    }



## ----------------------------------------------------------------------------
##
## SetupDBConnection
##
## $data_source  = Driver/DB/Host
##                  or recordset from which the data_source and dbhdl should be taken (optional)
## $table        = table (multiple tables must be comma separated)
## $username     = Username (optional)
## $password     = Password (optional) 
## \%attr        = Attributes (optional) 
##


sub SetupDBConnection($$$;$$\%)

    {
    my ($self, $data_source,  $table, $username, $password, $attr) = @_ ;

    $self->{'*Table'}      = $table ;
    $self->{'*Id'}         = $id++ ;

    if (ref ($data_source) eq 'DBIx::Recordset')
        { # copy from another recordset
        $self->{'*Driver'}     = $data_source->{'*Driver'} ;   
        $self->{'*DataSource'} = $data_source->{'*DataSource'} ;
        $self->{'*Username'}   = $data_source->{'*Username'} ; 
        $self->{'*DBHdl'}      = $data_source->{'*DBHdl'} ;    
        $self->{'*DBIAttr'}    = $data_source->{'*DBIAttr'} ;
        $self->{'*MainHdl'}    = 0 ;
        }
    else
        {
        $self->{'*DataSource'} = $data_source ;
        $self->{'*Username'}   = $username ;
        $self->{'*DBIAttr'}    = $attr ;
        $self->{'*DBHdl'}      = undef ;
        }

    
    my $hdl ;

    if (!defined ($self->{'*DBHdl'}))
        {
        $hdl = $self->{'*DBHdl'}  = DBI->connect($self->{'*DataSource'}, $self->{'*Username'}, $password, $self->{'*DBIAttr'}) or return undef ;

        $self->{'*MainHdl'}    = 1 ;
        $self->{'*Driver'}     = $hdl->{Driver}->{Name} ;

        $numOpen++ ;

        print LOG "DB:  Successfull connect (id=$self->{'*Id'}, numOpen = $numOpen)\n" if ($self->{'*Debug'}) ;
        }
    else
        {
        $hdl = $self->{'*DBHdl'} ;
        print LOG "DB:  Use already open dbh (id=$self->{'*Id'}, numOpen = $numOpen)\n" if ($self->{'*Debug'}) ;
        }
            
    my $meta ;
    my $metakey = "$self->{'*DataSource'}//$self->{'*Table'}" ;
    
    $self->{'*NullOperator'} = defined ($DBIx::Compat::Compat{$self->{'*Driver'}}{NullOperator})?$DBIx::Compat::Compat{$self->{'*Driver'}}{NullOperator}:$DBIx::Compat::Compat{'*'}{NullOperator} ;    
    if (defined ($meta = $Metadata{$metakey}))
        {
        $self->{'*Names'}  = $meta->{'*Names'} ;
        $self->{'*Types'}  = $meta->{'*Types'} ;
        $self->{'*Quote'}  = $meta->{'*Quote'} ;
        print LOG "Use cached metadata for $metakey\n" if ($self->{'*Debug'} > 1) ;
        }
    else
        {
        my $sth ;
    
        my $ListFields = ($DBIx::Compat::Compat{$self->{'*Driver'}}{ListFields} || $DBIx::Compat::Compat{'*'}{ListFields}) ;    
        my $QuoteTypes = ($DBIx::Compat::Compat{$self->{'*Driver'}}{QuoteTypes} || $DBIx::Compat::Compat{'*'}{QuoteTypes}) ;    
        my $HaveTypes  = defined ($DBIx::Compat::Compat{$self->{'*Driver'}}{HaveTypes})?$DBIx::Compat::Compat{$self->{'*Driver'}}{HaveTypes}:$DBIx::Compat::Compat{'*'}{HaveTypes} ;    
        my @tabs = split (/\s*\,\s*/, $table) ;
        my $tab ;
        my %Quote = () ;
        foreach $tab (@tabs)
            {
            $sth = &{$ListFields}($hdl, $tab) or die "Cannot list fields for $data_source" ;
    
            my $types ;
            my $fields = $sth -> {NAME}  ;
            my $num = $#{$fields} + 1 ;
        
            if ($HaveTypes)
                {
                #print LOG "DB: Have Types for driver\n" ;
                $types = $sth -> {TYPE}  ;
                }
            else
                {
                #print LOG "DB: No Types for driver\n" ;
                # Drivers does not have fields types -> give him SQL_VARCHAR
                my $i ;
                $types = [] ;
                for ($i = 0; $i < $num; $i++)
                    { push @$types, DBI::SQL_VARCHAR (); }

                # Setup quoting for SQL_VARCHAR
                $QuoteTypes = { DBI::SQL_VARCHAR() => 1 } ;
                }
        
            push @{$self->{'*Names'}}, @{ $fields } ;
            push @{$self->{'*Types'}}, @{ $types } ;


            $sth -> finish ;

            # Set up a hash which tells us which fields to quote and which not
            # We setup two versions, one with tablename and one without
            my $col ;
            my $fieldname ;
            for ($col = 0; $col < $num; $col++ )
                {
                if ($self->{'*Debug'})
                    {
                    my $n = $$fields[$col] ;
                    my $t = $$types[$col] ;
                    print LOG "DB: TAB = $tab, COL = $col, NAME = $n, TYPE = $t" ;
                    }
                $fieldname = $$fields[$col] ;
                if ($$QuoteTypes{$$types[$col]})
                    {
                    print LOG " -> quote\n" if ($self->{'*Debug'}) ;
                    $Quote {lc("$tab.$fieldname")} = 1 ;
                    $Quote {lc("$fieldname")} = 1 ;
                    }
                else
                    {
                    print LOG "\n" if ($self->{'*Debug'}) ;
                    $Quote {lc("$tab.$fieldname")} = 0 ;
                    $Quote {lc("$fieldname")} = 0 ;
                    }
                }
            print LOG "No Fields found for $tab\n" if ($num == 0 && $self->{'*Debug'}) ;
            }
    
        print LOG "No Tables specified\n" if ($#tabs < 0 && $self->{'*Debug'}) ;

        $self->{'*Quote'} = \%Quote ;    

        $meta = {} ;
        $meta->{'*Names'}  = $self->{'*Names'} ;
        $meta->{'*Types'}  = $self->{'*Types'} ;
        $meta->{'*Quote'}  = $self->{'*Quote'} ;

        $Metadata{$metakey} = $meta ;
        }


    return $hdl ;
    }


## ----------------------------------------------------------------------------
##
## TIEARRAY
##
## tie an array to the object, object must be aready blessed
##
## tie @self, 'DBIx::Recordset', $self ;
##


sub TIEARRAY
    {
    my ($class, $arg) = @_ ;
    my $rs ;    
    
    if (ref ($arg) eq 'HASH')
        {
        $rs = DBIx::Recordset -> SetupObject ($arg) or return undef ;
        }
    elsif (ref ($arg) eq 'DBIx::Recordset')
        {
        $rs = $arg ;
        }
    else
        {
        croak ("Need DBIx::Recordset or setup parameter") ;
        }

    
    return $rs ;
    }


sub STORESIZE
    
    {
    my ($self, $size) = @_ ;

    $self -> ReleaseRecords if ($size == 0) ;
    }


## ----------------------------------------------------------------------------
##
## New
##
## creates an new recordset object and ties an array and an hash to it
##
## returns a typeglob which contains:
## scalar:  ref to new object
## array:   array tied to object
## hash:    hash tied to object
##
## $data_source  = Driver/DB/Host
## $table        = table (multiple tables must be comma separated)
## $username     = Username (optional)
## $password     = Password (optional) 
## \%attr        = Attributes (optional) 
##


sub New
    {
    my ($class, $data_source,  $table, $username, $password, $attr) = @_ ;
    
    my $self = {'*Debug' => $Debug} ;

    bless ($self, $class) ;

    my $rc = $self->SetupDBConnection ($data_source,  $table, $username, $password, $attr) ;
    
    $self->{'*Placeholders'}= $DBIx::Compat::Compat{$self->{'*Driver'}}{Placeholders} ;
    $self->{'*Placeholders'}= $DBIx::Compat::Compat{'*'}{Placeholders} if (!defined ($self->{'*Placeholders'})) ;    
    $self->{'*Placeholders'}= 0 if ($self->{'*Placeholders'} < 10) ; # only full support for placeholders works

    if ($self->{'*Debug'})
        {
        print LOG "DB:  ERROR open DB $data_source ($DBI::errstr)\n" if (!defined ($rc)) ;

        my $n = '' ;
        $n = ' NOT' if (!$self->{'*Placeholders'}) ;
        print LOG "DB:  New Recordset driver=$self->{'*Driver'}  placeholders$n supported\n" ;
        }

    return defined($rc)?$self:undef ;
    }

## ----------------------------------------------------------------------------
##
## Setup
##
## creates an new recordset object and ties an array and an hash to it
##
## Same as New, but parameters passed as hash:
##
## !DataSource  = Driver/DB/Host
##                or a Recordset object from which to take the DataSource, DBIAttrs and username
## !Username    = username
## !Password    = password
## !DBIAttr     = reference to a hash which is passed to the DBI connect method
##
## !Table       = Tablename, muliply tables are comma separated
## !Fields      = fields which should be return by a query
## !TabRelation = condition which describes the relation
##                between the given tables
## !TabJoin     = JOIN to use in table part of select statement
## !PrimKey     = name of primary key
## !StoreAll	= store all fetched data
##
## !Default     = hash with default record data
## !IgnoreEmpty = 1 ignore undef values, 2 ignore empty strings
##

sub SetupObject

    {
    my ($class, $parm) = @_ ;

    my $self = New ($class, $$parm{'!DataSource'}, $$parm{'!Table'}, $$parm{'!Username'}, $$parm{'!Password'}, $$parm{'!DBIAttr'}) or return undef ; 

    $self->{'*Fields'}      = $$parm{'!Fields'} ;
    $self->{'*TabRelation'} = $$parm{'!TabRelation'} ;
    $self->{'*TabJoin'}     = $$parm{'!TabJoin'} ;
    $self->{'*PrimKey'}     = $$parm{'!PrimKey'} ;
    $self->{'*StoreAll'}    = $$parm{'!StoreAll'} ;
    $self->{'*Default'}     = $$parm{'!Default'} if (defined ($$parm{'!Default'})) ;
    $self->{'*IgnoreEmpty'} = $$parm{'!IgnoreEmpty'} || 0 ;
    $Data{$self->{'*Id'}}   = [] ;
    $self->{'*FetchStart'}  = 0 ;
    $self->{'*FetchMax'}    = undef ;
    $self->{'*EOD'}         = undef ;
    $self->{'*CurrRow'}     = 0 ;
    my $links = $self->{'*Links'}       = $$parm{'!Links'} ;
    if (defined ($links))
        {
        my $k ;
        my $v ;
        while (($k, $v) = each (%$links))
            {
            $v -> {'!LinkedField'} = $v -> {'!MainField'} if (defined ($v) && !defined ($v -> {'!LinkedField'})) ;
            $v -> {'!MainField'}   = $v -> {'!LinkedField'} if (defined ($v) && !defined ($v -> {'!MainField'})) ;
            }
        }

    return $self ;
    }


sub Setup

    {
    my ($class, $parm) = @_ ;

    local *self ;
    
    $self = SetupObject ($class, $parm) or return undef ;

    tie @self, $class, $self ;
    if ($parm -> {'!HashAsRowKey'})
	{
	tie %self, "$class\:\:Hash", $self ;
	}
    else
	{
	tie %self, "$class\:\:CurrRow", $self ;
	}

    return *self ;
    }


## ----------------------------------------------------------------------------
##
## ReleaseRecords ...
##
## Release all records, write data if necessary
##

sub ReleaseRecords

    {
    undef $_[0] -> {'*LastKey'} ;
    $_[0] -> Flush (1) ;
    $Data{$_[0] -> {'*Id'}} = [] ;
    }



## ----------------------------------------------------------------------------
##
## undef and untie the object
##

sub Undef

    {
    my ($objname) = @_ ;

    if (!($objname =~ /\:\:/))
        {
        my ($c) = caller () ;
        $objname = "$c\:\:$objname" ;
        } 
    
    print LOG "DB:  Undef $objname\n" if (defined (${$objname}) && (${$objname}->{'*Debug'} || $Debug)) ; 
    
    
    if (defined (${$objname})) 
        {
        # Cleanup rows and write them if necessary
        ${$objname} -> ReleaseRecords () ;
        ${$objname} -> Disconnect () ;
        }

    if (defined (%{$objname}))
        {
        my $obj = tied (%{$objname}) ;
        $obj -> {'*Recordset'} = undef if ($obj) ;
        $obj = undef ;
        }

    #${$objname} = undef ;
    untie %{$objname} ;
    undef ${$objname} ;
    untie @{$objname} ;
    }


## ----------------------------------------------------------------------------
##
## disconnect from database
##

sub Disconnect ($)
    {
    my ($self) = @_ ;

    if (defined ($self->{'*StHdl'})) 
        {
        $self->{'*StHdl'} -> finish () ;
        undef $self->{'*StHdl'} ;
        }

    $self -> ReleaseRecords () ;

    if (defined ($self->{'*DBHdl'}) && $self->{'*MainHdl'})
        {
        $numOpen-- ;
        $self->{'*DBHdl'} -> disconnect () ;
        undef $self->{'*DBHdl'} ;
        }


    print LOG "DB:  Disconnect (id=$self->{'*Id'}, numOpen = $numOpen)\n" if ($self->{'*Debug'}) ;
    }


## ----------------------------------------------------------------------------
##
## do some cleanup 
##

sub DESTROY ($)
    {
    my ($self) = @_ ;

    $self -> Disconnect () ;

    print LOG "DB:  DESTROY (id=$self->{'*Id'}, numOpen = $numOpen)\n" if ($self->{'*Debug'}) ;
    }

## ----------------------------------------------------------------------------
##
## store something in the array
##

sub STORE 

    {
    my ($self, $fetch, $value) = @_ ;

    $fetch += $self->{'*FetchStart'} ;
    #$max    = $self->{'*FetchMax'} ;
    print LOG "DB:  STORE \[$fetch\] = $value\n"  if ($self->{'*Debug'} > 1) ;
    if ($self->{'*Debug'} > 1 && ref ($value) eq 'HASH')
        {
        my $k ;
        my $v ;
        while (($k, $v) = each (%$value))
            {
            print LOG "<$k>=<$v> " ;
            }
        print LOG "\n" ;
        }        
    my $r ;
    my $rec ;
    $value ||= {} ;
    if (keys %$value)
        {
        my %rowdata ;
        $r = tie %rowdata, 'DBIx::Recordset::Row', $self ;
        %rowdata = %$value ;
        $rec = $Data{$self->{'*Id'}}[$fetch] = \%rowdata ;
        }
    else
        {
        $r = tie %$value, 'DBIx::Recordset::Row', $self, $value ;
        $rec = $Data{$self->{'*Id'}}[$fetch] = $value ;
        %$value = %{$self -> {'*Default'}} if (exists ($self -> {'*Default'})) ;
        }
    $r -> {'*new'} = 1 ;

    #$self->{'*LastRow'} = $fetch ;
    #$self->{'*LastKey'} = $r -> FETCH ($self -> {'*PrimKey'}) ;

    return $rec ;
    } 

## ----------------------------------------------------------------------------
##
## Add
##
## Add a new record
##

sub Add
    
    {
    my ($self, $data) = @_ ;

    my $num = $#{$Data{$self->{'*Id'}}} + 1 ;

    $self -> STORE ($num, $data) if ($data) ;
    
    $self -> {'*CurrRow'} = $num + 1 ;
    $self -> {'*LastRow'} = $num ;
    
    return $num ;
    }

## ----------------------------------------------------------------------------
##
## DBHdl
##
## return DBI database handle
##

sub DBHdl ($)

    {
    return $_[0] -> {'*DBHdl'} ;
    }


## ----------------------------------------------------------------------------
##
## StHdl
##
## return DBI statement handle of last select
##

sub StHdl ($)

    {
    return $_[0] -> {'*StHdl'} ;
    }




## ----------------------------------------------------------------------------
##
## do an non select statement 
##
## $statement = statement to do
## \%attr     = attribs (optional)
## @bind_valus= values to bind (optional)
##

sub do($$;$$)

    {
    my($self, $statement, $attribs, @params) = @_;
    
    print LOG "DB:  do $statement <@params>\n" if ($self->{'*Debug'}) ;
    
    $self -> {'*LastSQLStatement'} = $statement ;

    my $ret = $self->{'*DBHdl'} -> do ($statement, $attribs, @params) ;

    print LOG "DB:  do returned $ret\n" if ($self->{'*Debug'}) ;
    print LOG "DB:  ERROR $DBI::errstr\n"  if (!$ret && $self->{'*Debug'}) ;
    
    return $ret ;
    }

## ----------------------------------------------------------------------------
##
## AllNames
##
## return reference to array of all names in all tables
##

sub AllNames

    {
    return $_[0] -> {'*Names'} ;
    }

## ----------------------------------------------------------------------------
##
## AllTypes
##
## return reference to array of all types in all tables
##

sub AllTypes

    {
    return $_[0] -> {'*Types'} ;
    }


## ----------------------------------------------------------------------------
##
## Names
##
## return reference to array of names of the last query
##

sub Names

    {
    my $sth = $_[0] -> {'*StHdl'} ;
    return undef if (!$sth) ;
    return $sth -> FETCH('NAME') ;
    }


## ----------------------------------------------------------------------------
##
## Types
##
## return reference to array of types of the last query
##

sub Types

    {
    my $sth = $_[0] -> {'*StHdl'} ;
    return undef if (!$sth) ;
    return $sth -> FETCH('TYPE') ;
    }


## ----------------------------------------------------------------------------
##
## StartRecordNo
##
## return the record no which will be returned for index 0
##

sub StartRecordNo

    {
    return $_[0] -> {'*FetchStart'} ;
    }

## ----------------------------------------------------------------------------
##
## LastSQLStatement
##
## return the last executet SQL Statement
##

sub LastSQLStatement

    {
    return $_[0] -> {'*LastSQLStatement'} ;
    }



## ----------------------------------------------------------------------------
##
## SQL Insert ...
##
## $fields = comma separated list of fields to insert
## $vals   = comma separated list of values to insert
## \@bind_values = values which should be insert for placeholders
##

sub SQLInsert ($$$$)

    {
    my ($self, $fields, $vals, $bind_values) = @_ ;
    
    return $self->do ("INSERT INTO $self->{'*Table'} ($fields) VALUES ($vals)", undef, @$bind_values) ;
    }

## ----------------------------------------------------------------------------
##
## SQL Update ...
##
## $data = komma separated list of fields=value to update
## $where = SQL Where condition
## \@bind_values = values which should be insert for placeholders
##
##

sub SQLUpdate ($$$$)

    {
    my ($self, $data, $where, $bind_values) = @_ ;
    
    return $self->do ("UPDATE $self->{'*Table'} SET $data WHERE $where", undef, @$bind_values) ;
    }

## ----------------------------------------------------------------------------
##
## SQL Delete ...
##
## $where = SQL Where condition
## \@bind_values = values which should be insert for placeholders
##
##

sub SQLDelete ($$$)

    {
    my ($self, $where, $bind_values) = @_ ;
    
    return $self->do ("DELETE FROM $self->{'*Table'} WHERE $where", undef, @$bind_values) ;
    }




## ----------------------------------------------------------------------------
##
## SQL Select
##
## Does an SQL Select of the form
##
##  SELECT $fields FROM <table> WHERE $expr ORDERBY $order
##
## $expr    = SQL Where condition (optional, defaults to no condition)
## $fields  = fields to select (optional, default to *)
## $order   = fields for sql order by or undef for no sorting (optional, defaults to no order) 
## $group   = fields for sql group by or undef (optional, defaults to no grouping) 
## $append  = append that string to the select statemtn for other options (optional) 
## \@bind_values = values which should be inserted for placeholders
##

sub SQLSelect ($;$$$$$$)
    {
    my ($self, $expr, $fields, $order, $group, $append, $bind_values) = @_ ;

    my $sth ;  # statement handle
    my $where ; # where or nothing
    my $orderby ; # order by or nothing
    my $groupby ; # group by or nothing
    my $rc  ;        #
    my $table ;

    $self->{'*StHdl'} -> finish () if (defined ($self->{'*StHdl'})) ;
    undef $self->{'*StHdl'} ;
    $self->ReleaseRecords ;
    undef $self->{'*LastKey'} ;
    $self->{'*FetchStart'} = 0 ;
    $self->{'*FetchMax'} = undef ;
    $self->{'*EOD'} = undef ;

    $order  ||= '' ;
    $expr   ||= '' ;
    $orderby  = $order?'ORDER BY':'' ;
    $groupby  = $group?'GROUP BY':'' ;
    $where    = $expr?'WHERE':'' ;
    $fields ||= '*';
    $table    = $self->{'*TabJoin'} || $self->{'*Table'} ;

    my $statement = "SELECT $fields FROM $table $where $expr $orderby $order $groupby $group $append" ;

    if ($self->{'*Debug'})
        { 
        my $b = $bind_values || [] ;
        print LOG "DB:  $statement <@$b>\n" ;
        }

    $self -> {'*LastSQLStatement'} = $statement ;

    $sth = $self->{'*DBHdl'} -> prepare ($statement) ;

    if (defined ($sth))
        {
        $rc = $sth -> execute (@$bind_values) ;
	}
        
    
    if ($rc)
    	{
    	$self->{'*NumFields'} = $#{$sth -> FETCH ('NAME')} + 1 ;
	}
    else
    	{
        print LOG "DB:  ERROR $DBI::errstr\n" if ($self->{'*Debug'}) ;
    
    	$self->{'*NumFields'} = 0 ;
	
	undef $sth ;
	}

    $self->{'*CurrRow'} = 0 ;
    $self->{'*LastRow'} = 0 ;
    $self->{'*StHdl'}   = $sth ;
    
    return $rc ;
    }

## ----------------------------------------------------------------------------
##
## Fetch the data from a previous SQL Select
##
## $fetch     = Row to fetch
## 
## fetchs the nth row and return a ref to an hash containing the entire row data
##


sub FETCH  
    {
    my ($self, $fetch) = @_ ;

    my $max ;
    my $key ;
    my $dat ;                           # row data

    
    print LOG "DB:  FETCH \[$fetch\]\n"  if ($self->{'*Debug'} > 1) ;


    $fetch += $self->{'*FetchStart'} ;
    $max    = $self->{'*FetchMax'} ;

    my $row = $self->{'*CurrRow'} ;     # row next to fetch from db
    my $sth = $self->{'*StHdl'} ;       # statement handle
    my $data = $Data{$self->{'*Id'}} ;  # data storage (Data is stored in a seperate hash to avoid circular references)

    if ($row <= $fetch && !$self->{'*EOD'} && defined ($sth))
        {

        # successfull select has happend before ?
        return undef if (!defined ($sth)) ;
        return undef if (defined ($max) && $row > $max) ;
        
        my $fld = $sth -> FETCH ('NAME') ;
        my $arr  ;
        my $i  ;

	if ($self -> {'*StoreAll'})
	    {
	    while ($row < $fetch)
		{
    	        if (!($arr = $sth -> fetchrow_arrayref ()))
		    {
		    $self->{'*EOD'} = 1 ;
		    last ;
		    }
                
                $i = 0 ;
                %{$data->[$row]} = map { lc($$fld[$i++]) => $_ } @$arr ;
		$row++ ;

                last if (defined ($max) && $row > $max) ;
		}
	    }
	else
	    {
	    while ($row < $fetch)
		{
    	        if (!$sth -> fetchrow_arrayref ())
		    {
		    $self->{'*EOD'} = 1 ;
		    last ;
		    }
		$row++ ;
                last if (defined ($max) && $row > $max) ;
		}
	    }


        $self->{'*LastRow'}   = $row ;
        if ($row == $fetch && !$self->{'*EOD'})
    	    {
            
    	    $arr = $sth -> fetchrow_arrayref () ;
            
            if ($arr)
                {
                $row++ ;
                $dat = {} ;
                my $obj = tie %$dat, 'DBIx::Recordset::Row', $self, $fld, $arr ;
                
                #tie %$dat, 'DBIx::Recordset::Row', $self, ['id'], [333] ;
                #print LOG "new dat = $dat  row = $row  fetch=$fetch  ref = " . ref ($dat) . " tied = " . ref (tied(%$dat)) . " fetch = $fetch  self = $self\n"  ;
                #return $h ;
                #my $i  ;
                #my %p ;
                #for ($i = 0; $i <= $#{$fld}; $i++)
                #    {
                #    print "arr  $$fld[$i] = $$arr[$i]\n" ;
                #    }
                #
                #my $v ;
                #my $k ;
                #while (($k, $v) = each (%$dat))
                #    {
                #    print "hash $k = $v\n" ;
                #    }

                #
                #tie %$h, 'DBIx::Recordset::Row', $self, \%p ;
                
                $data -> [$fetch] = $dat ;
                $self->{'*LastKey'} = $obj -> FETCH ($self -> {'*PrimKey'}) ;
                }
            else
                {
                $dat = $data -> [$fetch] = undef ;
                #print LOG "new dat undef\n"  ;
    	        $self->{'*EOD'} = 1 ;
                }
            }
        $self->{'*CurrRow'} = $row ;
        }
    else
        {
        my $v ;
        my $k ;

        $dat = $data -> [$fetch] ;
        #print LOG "old dat\n" ; #  = $dat  ref = " . ref ($dat) . " tied = " . ref (tied(%$dat)) . " fetch = $fetch\n"  ;
        #while (($k, $v) = each (%$dat))
        #        {
        #        print "$k = $v\n" ;
        #        }


        my $obj = tied(%$dat) if ($dat) ;
        $self->{'*LastRow'} = $fetch ;
        $self->{'*LastKey'} = $obj?($obj -> FETCH ($self -> {'*PrimKey'})):undef ;
        }

    print LOG 'DB:  FETCH return ' . ($dat?$dat:'<undef>') . "\n"  if ($self->{'*Debug'} > 1) ;
    return $dat ;
    }


## ----------------------------------------------------------------------------
## 
## First ...
##
## position the record pointer to the first row and return it
##

sub First ($;$)
    {
    my $rec = $_[0] -> FETCH (0) ;
    return $rec if (defined ($rec) || !$_[1]) ;

    # create new record 
    return $_[0] -> STORE (0) ;
    }


## ----------------------------------------------------------------------------
## 
## Last ...
##
## position the record pointer to the last row
## DOES NOT WORK!!
##
##

sub Last ($)
    {
    $_[0] -> FETCH (0x7fffffff) ; # maxmimun postiv integer
    return undef if ($_[0] -> {'*LastRow'} == 0) ;
    return $_[0] -> Prev ;
    }


## ----------------------------------------------------------------------------
## 
## Next ...
##
## position the record pointer to the next row and return it
##

sub Next ($;$)
    {
    my $n = $_[0] ->{'*LastRow'} - $_[0] -> {'*FetchStart'}  ;
    $n++ if ($_[0] ->{'*CurrRow'} > 0 || $_[0] ->{'*EOD'}) ; 
    my $rec = $_[0] -> FETCH ($n) ;
    return $rec if (defined ($rec) || !$_[1]) ;

    # create new record 
    return $_[0] -> STORE ($n) ;
    }


## ----------------------------------------------------------------------------
## 
## Prev ...
##
## position the record pointer to the previous row and return it
##

sub Prev ($)
    {
    $_[0] -> {'*LastRow'} = 0 if (($_[0] -> {'*LastRow'})-- == 0) ;
    return $_[0] -> FETCH ($_[0] ->{'*LastRow'} - $_[0] -> {'*FetchStart'}) ;
    }


## ----------------------------------------------------------------------------
##
## Fetch the data from current row
##


sub Curr ($;$)
    {
    my $n = $_[0] ->{'*LastRow'} - $_[0] -> {'*FetchStart'} ;
    my $rec = $_[0] -> FETCH ($n) ;
    return $rec if (defined ($rec) || !$_[1]) ;

    # create new record 
    return $_[0] -> STORE ($n) ;
    }


## ----------------------------------------------------------------------------
## 
## BuildWhere ...
##
## \%where/$where   = hash of which the SQL Where condition is build
##                    or SQL Where condition as text
## \@bind_values    = returns the bind_value array if placeholder supported
##
##
## Builds the WHERE condition for SELECT, UPDATE, DELETE 
## upon the data which is given in the hash \%where or string $where
##
##      Key                 Value
##      <fieldname>         Value for field (automatily quote if necessary)
##      '<fieldname>        Value for field (always quote)
##      #<fieldname>        Value for field (never quote, convert to number)
##      \<fieldname>        Value for field (leave value as it is)
##      +<field>|<field>..  Value for fields (value must be in one/all fields
##                          depending on $compconj
##      $compconj           'or' or 'and' (default is 'or') 
##
##      $valuesplit         regex for spliting a field value in mulitply value
##                          per default one of the values must match the field
##                          could be changed via $valueconj
##      $valueconj          'or' or 'and' (default is 'or') 
##
##      $conj               'or' or 'and' (default is 'and') conjunction between
##                          fields
##
##      $operator           Default operator
##      *<fieldname>        Operator for the named field
##
##	$primkey	    primary key
##
##	$where		    where as string
##

sub BuildWhere ($$)

    {
    my ($self, $where, $bind_values) = @_ ;
    
    
    my $expr = '' ;
    my $primkey ;
    my $Quote = $self->{'*Quote'} ;
    my $Debug = $self->{'*Debug'} ;
    my $placeholders = $self->{'*Placeholders'} ;
    my $ignore       = $self->{'*IgnoreEmpty'} ;
    my $nullop       = $self->{'*NullOperator'} ;
        

    if (!ref($where))
        { # We have the where as string
        $expr = $where ;
        if ($Debug > 1) { print LOG "DB:  Literal where -> $expr\n" ; }
        }
    elsif (defined ($primkey = $self->{'*PrimKey'}) && defined ($$where{$primkey}))
        {
        my $oper = $$where{"\*$primkey"} || '=' ;
        if ($placeholders)
            { $expr = "$primkey$oper ? "; push @$bind_values, $$where{$primkey} ; }
        elsif ($$Quote{$primkey})
            { $expr = "$primkey$oper" . $self->{'*DBHdl'} -> quote ($$where{$primkey}) ; }
        else        
            { $expr = "$primkey$oper" . ($$where{$primkey}+0) ; }
        if ($Debug > 1) { print LOG "DB:  Primary Key $primkey found -> $expr\n" ; }
        }
    else
        {         
        my $key ;
        my $val ;

        my @mvals ;
    
        my $field ;
        my @fields ;

        my $econj ;
        my $vconj ;
        my $fconj ;
    
        my $vexp  ;
        my $fieldexp  ;

        my $type ;
        my $oper = $$where{'$operator'} || '=' ;
        my $op ;

        my $mvalsplit = $$where{'$valuesplit'} || "\t" ;

        my $lexpr = '' ;
        my $multcnt ;

        $econj = '' ;
    
 
        while (($key, $val) = each (%$where))
            {
            $type  = substr ($key, 0, 1) || ' ' ;
            $val = undef if ($ignore > 1 && $val eq '') ;

            if ($Debug) { print LOG "DB:  SelectWhere <$key>=<$val> type = $type\n" ; }

            if (($type =~ /^(\w|\\|\+|\'|\#|\s)$/) && !($ignore && !defined ($val)))
                {
                if ($type eq '+')
                    { # composite field
                
                    if ($Debug > 1) { print LOG "DB:  Composite Field $key\n" ; }

                    $fconj    = '' ;
                    $fieldexp = '' ;
                    @fields   = split (/\&|\|/, substr ($key, 1)) ;

                    $multcnt = 0 ;
                    foreach $field (@fields)
                        {
                        if ($Debug > 1) { print LOG "DB:  Composite Field processing $field\n" ; }

                        if (!defined ($$Quote{lc($field)}))
                            {
                            if ($Debug) { print LOG "DB:  Ignore non existing Composite Field $field\n" ; }
                            next ;
                            } # ignore no existent field

                        $op = $$where{"*$field"} || $oper ;
                        if ($placeholders && $type ne '\\')
                            { $fieldexp = "$fieldexp $fconj $field $op \$val" ; $multcnt++ ; }
                        elsif (!defined ($val))
                            { $fieldexp = "$fieldexp $fconj $field $nullop NULL" ; }
                        elsif ($$Quote{lc($field)} && $type ne '\\')
                            { $fieldexp = "$fieldexp $fconj $field $op '\$val'" ; }
                        else
                            { $fieldexp = "$fieldexp $fconj $field $op \" . (\$val+0) . \"" ; }

                        $fconj ||= $$where{'$compconj'} || ' or ' ; 

                        if ($Debug > 1) { print LOG "DB:  Composite Field get $fieldexp\n" ; }

                        }
                    if ($fieldexp eq '')
                        { next ; } # ignore no existent field

                    }
                else
                    { # single field
                    $multcnt = 1 ;
                    if ($type eq '\\' || $type eq '#' || $type eq "'")
                        { # remove leading backslash, # or '
                        $key = substr ($key, 1) ;
                        }

                    if ($type eq "'")
                        {
                        $$Quote{lc($key)} = 1 ;
                        }
                    elsif ($type eq '#')
                        {
                        $$Quote{lc($key)} = 0 ;
                        }

                    #$val += 0 if ($$Quote{lc($key)}) ; # convert value to a number if necessary
        
                    if (!defined ($$Quote{lc($key)}) && $type ne '\\')
                        {
                        if ($Debug > 1) { print LOG "DB:  Ignore Single Field $key\n" ; }
                        next ; # ignore no existent field
                        } 

                    if ($Debug > 1) { print LOG "DB:  Single Field $key\n" ; }

                    $op = $$where{"*$key"} || $oper ;
                    if (!$placeholders && defined ($val) && $$Quote{lc($key)} && $type ne '\\')
                        { $fieldexp = "$key $op '\$val'" ; }
                    elsif (defined ($val) || $placeholders)
                        { $fieldexp = "$key $op \$val" ; }
                    else
                        { $fieldexp = "$key $nullop NULL" ; }

                    if ($Debug > 1) { print LOG "DB:  Single Field gives $fieldexp\n" ; }
                    }
    
    
                @mvals = split (/$mvalsplit/, $val) ;
                if ($#mvals > 0)
                    { # multiplie values for that field
                
                    $vexp  = '' ;
                    $vconj = '' ;
                
                    foreach $val (@mvals)
                        {
                        if ($placeholders)
                            {
                            my $i ;

                            for ($i = 0; $i < $multcnt; $i++)
                                { push @$bind_values, $val ; }
                            $val = '?' ;
                            }
                        elsif (!defined ($val))
                            {
                            $val = 'NULL' ;
                            }
                                                
                        $vexp = "$vexp $vconj " . eval "\"($fieldexp)\"" ;
                        $vconj ||= $$where{'$valueconj'} || ' or ' ; 
                        }                
                    }
                else
                    {
                    if ($placeholders && $type ne '\\')
                        {
                        my $i ;
                        
                        for ($i = 0; $i < $multcnt; $i++)
                            { push @$bind_values, $val ; }
                        $val = '?' ;
                        }
                    elsif (!defined ($val))
                        {
                        $val = 'NULL' ;
                        }
                        
                    $vexp = eval "\"($fieldexp)\"" ;
                    }

                if ($Debug > 1) { print LOG "DB:  Key $key gives $vexp\n" ; }
            
            
                $expr = "$expr $econj ($vexp)" ;
            
                $econj ||= $$where{'$conj'} || ' and ' ; 
                }
            if ($Debug > 1 && $lexpr ne $expr) { $lexpr = $expr ; print LOG "DB:  expr is $expr\n" ; }
            }
        }


    # Now we add the Table relations, if any

    my $tabrel = $self->{'*TabRelation'} ;

    if ($tabrel)
        {
        if ($expr)
            {
            $expr = "($tabrel) and ($expr)" ;
            }
        else
            {
            $expr = $tabrel ;
            }
        }
    
    return $expr ;
    }

## ----------------------------------------------------------------------------
##
## Check fields ...
##
## delete all fields which do not belong to this recordset from hashref $data
##


sub CheckFields ($\%\%)

    {
    my ($self, $data, $cdata) = @_ ;
    
    my $key ;
    my $val ;

    my $Quote = $self->{'*Quote'} ;

    while (($key, $val) = each (%$data))
        {
        if (defined ($$Quote{lc($key)}))
            { 
            $$cdata{$key} = $val ;
            }
        elsif ($self->{'*Debug'})  { print LOG "DB:  CheckFields del $key = $val \n" ; }
        }

    }

## ----------------------------------------------------------------------------
##
## Fush ...
##
## Write all dirty rows to the database
##

sub Flush

    {
    my $self    = shift ;
    my $release = shift ;
    my $dat ;
    my $obj ;
    my $data = $Data{$self->{'*Id'}} ;
    my $rc = 1 ;

    foreach $dat (@$data)
	{
        $obj = tied (%$dat) ;
        #print LOG "FLUSH RS dat=$dat " . ref ($obj) . "\n" ;
        
        if (defined ($obj)) 
            {
            #print "rs=" . ref ($obj->{'*Recordset'}) . "\n" ; 
            
            #Devel::Peek::Dump ($obj -> {'*Recordset'}, 1) ;
            
            $obj -> Flush () or $rc = undef ;
            $obj -> {'*Recordset'} = undef if ($release) ;
            }


	#if ($dat && !$obj)
        # 		{
	#	print LOG "FLUSH RS untied hash\n" ;
	#	my $k ;
	#	my $v ;
	#	
	#	while (($k, $v) = each (%$dat))
	#		{
	#		print "$k = $v\n" ;
	#		}
	#	}
	#		

	}

    return $rc ;
    }




## ----------------------------------------------------------------------------
##
## Insert ...
##
## \%data = hash of fields for new record
##

sub Insert ($\%)

    {
    my ($self, $data) = @_ ;

    local *newself ;
    if (!ref ($self)) 
        {
        *newself = Setup ($self, $data) ;
        ($self = $newself) or return undef ;
        }

    my $placeholders = $self->{'*Placeholders'} ;

    my @bind_values ;
    my @qvals ;
    my @keys ;
    my $key ;
    my $val ;
    my $q ;

    my $Quote = $self->{'*Quote'} ;

    if ($placeholders)
        {
        while (($key, $val) = each (%$data))
            {
            $val = $$val if (ref ($val) eq 'SCALAR') ;
            if (defined ($$Quote{lc($key)}))
                {
                push @bind_values ,$val ;
                push @qvals, '?' ;
                push @keys, $key ;
                }
            }
        }
    else
        {
        while (($key, $val) = each (%$data))
            {
            $val = $$val if (ref ($val) eq 'SCALAR') ;
            if (($q = $$Quote{lc($key)}))
                {
                push @qvals, $self->{'*DBHdl'} -> quote ($val) ;
                push @keys, $key ;
                }
            elsif (defined ($q))
                {
                push @qvals, "$val" ;
                push @keys, $key ;
                }
            }
        }

    my $valstr = join (',', @qvals) ;
    my $keystr = join (',', @keys) ;

    my $rc = $self->SQLInsert ($keystr, $valstr, \@bind_values) ;
    return ($newself && defined ($rc))?*newself:$rc ;
    }

## ----------------------------------------------------------------------------
##
## Update ...
##
## \%data = hash of fields for new record
## $where/\%where = SQL Where condition
##
##

sub Update ($\%$)

    {
    my ($self, $data, $where) = @_ ;
    
    local *newself ;
    if (!ref ($self)) 
        {
        *newself = Setup ($self, $data) ;
        ($self = $newself) or return undef ;
        }

    my $expr  ;
    my @bind_values ;
    my $key ;
    my $val ;
    my @vals ;
    my $q ;

    my $Quote = $self->{'*Quote'} ;
    my $placeholders = $self->{'*Placeholders'} ;

    if ($placeholders)
        {
        while (($key, $val) = each (%$data))
            {
            $val = $$val if (ref ($val) eq 'SCALAR') ;
            if (defined ($$Quote{lc($key)}))
                { 
                push @vals, "$key=?" ;
                push @bind_values, $val ;
                }
            }
        }
    else
        {
        while (($key, $val) = each (%$data))
            {
            $val = $$val if (ref ($val) eq 'SCALAR') ;
            if (($q = $$Quote{lc($key)}))
                { push @vals, "$key=" . $self->{'*DBHdl'} -> quote ($val) ; }
            elsif (defined ($q))
                { push @vals, "$key=$val" ; }
            }
        }

    my $valstr = join (',', @vals) ;

    if (defined ($where))
        { $expr = $self->BuildWhere ($where, \@bind_values) ; }
    else
        { $expr = $self->BuildWhere ($data, \@bind_values) ; }


    my $rc = $self->SQLUpdate ($valstr, $expr, \@bind_values) ;
    return ($newself && defined ($rc))?*newself:$rc ;
    }



## ----------------------------------------------------------------------------
##
## UpdateInsert ...
##
## First try an update, if this fail insert an new record
##
## \%data = hash of fields for record
##

sub UpdateInsert ($\%)

    {
    my ($self, $fdat) = @_ ;

    my $rc ;

    local *newself ;
    if (!ref ($self)) 
        {
        *newself = Setup ($self, $fdat) ;
        ($self = $newself) or return undef ;
        }

    $rc = $self -> Update ($fdat) ;
    print LOG "DB:  UpdateInsert update returns: $rc  affected rows: $DBI::rows\n" if ($self->{'*Debug'}) ;
    
    if (!$rc || $DBI::rows <= 0)
        {
        $rc = $self -> Insert ($fdat) ;
        }
    return ($newself && defined ($rc))?*newself:$rc ;
    }




## ----------------------------------------------------------------------------
##
## Delete ...
##
## $where/\%where = SQL Where condition
##
##

sub Delete ($$)

    {
    my ($self, $where) = @_ ;
    
    local *newself ;
    if (!ref ($self)) 
        {
        *newself = Setup ($self, $where) ;
        ($self = $newself) or return undef ;
        }

    my @bind_values ;
    my $expr = $self->BuildWhere ($where,\@bind_values) ;

    my $rc = $self->SQLDelete ($expr, \@bind_values) ;
    return ($newself && defined ($rc))?*newself:$rc ;
    }


## ----------------------------------------------------------------------------
##
## Select
##
## Does an SQL Select of the form
##
##  SELECT $fields FROM <table> WHERE $expr ORDERBY $order
##
## $where/%where = SQL Where condition (optional, defaults to no condition)
## $fields       = fields to select (optional, default to *)
## $order        = fields for sql order by or undef for no sorting (optional, defaults to no order) 
## $group        = fields for sql group by or undef (optional, defaults to no grouping) 
## $append       = append that string to the select statemtn for other options (optional) 
##


sub Select (;$$$$$)
    {
    my ($self, $where, $fields, $order, $group, $append) = @_ ;

    local *newself ;
    if (!ref ($self)) 
        {
        *newself = Setup ($self, $where) ;
        ($self = $newself) or return undef ;
        }

    my @bind_values ;
    my $expr = $self->BuildWhere ($where, \@bind_values) ;

    my $rc = $self->SQLSelect ($expr, $self->{'*Fields'} || $fields, $order, $group, $append, \@bind_values) ;
    return ($newself && defined ($rc))?*newself:$rc ;
    }


## ----------------------------------------------------------------------------
##
## Search data
##
## \%fdat   = hash of form data
##      
##   Special keys in hash:
##      $start: first row to fetch 
##      $max:   maximum number of rows to fetch
##	$next:	next n records
##	$prev:	previous n records
##	$order: fieldname(s) for ordering (could also contain USING)
##      $group: fields for sql group by or undef (optional, defaults to no grouping) 
##      $append:append that string to the select statemtn for other options (optional) 
##      $fields:fieldnams(s) to retrieve    
##



sub Search ($\%)

    {
    my ($self, $fdat) = @_ ;

    local *newself ;
    if (!ref ($self)) 
        {
        *newself = Setup ($self, $fdat) ;
        ($self = $newself) or return undef;
        }

    my $Quote = $self->{'*Quote'} ;

    my $start = $$fdat{'$start'} || 0 ;
    my $max   = $$fdat{'$max'} ;

    $start = 0 if (defined ($start) && $start < 0) ;
    $max   = 1 if (defined ($max) && $max < 1) ;

    if (defined ($$fdat{'$prev'}))
        {
        $start -= $max ; 
        if ($start < 0) { $start = 0 ; }
        }
    elsif (defined ($$fdat{'$next'}))
        { $start += $max ; }

    my $rc = $self->Select($fdat, $$fdat{'$fields'}, $$fdat{'$order'}, $$fdat{'$group'}, $$fdat{'$append'}) ; 
    $self->{'*FetchStart'} = $start ;
    $self->{'*FetchMax'}   = $start + $max - 1 if (defined ($max)) ;
    
    return ($newself && defined ($rc))?*newself:$rc ;
    }




## ----------------------------------------------------------------------------
##
## Execute
##
##
## \%fdat   = hash of form data
##
##      =search  = search data
##      =update  = update record(s)
##      =insert  = insert record
##      =delete  = delete record(s)
##      =empty   = setup empty object
##


sub Execute ($\%)

    {
    my ($self, $fdat) = @_ ;

    local *newself ;
    if (!ref ($self)) 
        {
        *newself = Setup ($self, $fdat) ;
        ($self = $newself)  or return undef ;
        }


    if ($self->{'*Debug'})
         { print LOG 'DB:  Execute ' . ($$fdat{'=search'}?'=search ':'') .
                ($$fdat{'=update'}?'=update ':'') . ($$fdat{'=insert'}?'=insert ':'') .
                ($$fdat{'=empty'}?'=empty':'') . ($$fdat{'=delete'}?'=delete':'') . "\n" ; }

    my $rc ;
    $rc = $self -> Search ($fdat) if (defined ($$fdat{'=search'})) ;
    #$rc = $self -> UpdateInsert ($fdat) if (defined ($$fdat{'=update'}) && defined ($$fdat{'=insert'}) && !defined($rc)) ;
    $rc = $self -> Update ($fdat) if (defined ($$fdat{'=update'}) && !defined($rc)) ;
    $rc = $self -> Insert ($fdat) if (defined ($$fdat{'=insert'}) && !defined($rc)) ;
    $rc = $self -> Delete ($fdat) if (defined ($$fdat{'=delete'}) && !defined($rc)) ;
    $rc = $self -> Search ($fdat) if (!defined ($$fdat{'=empty'}) && !defined($rc)) ;
        
    return ($newself && defined ($rc))?*newself:$rc ;
    }

## ----------------------------------------------------------------------------
##
## PushCurrRec
##

sub PushCurrRec

    {
    my ($self) = @_ ;

    # Save Current Record
    my $sp = $self->{'*CurrRecStack'} ;
    push @$sp, $self->{'*LastRow'} ;
    push @$sp, $self->{'*LastKey'} ;
    push @$sp, $self->{'*FetchMax'} ;
    }



## ----------------------------------------------------------------------------
##
## PopCurrRec
##

sub PopCurrRec

    {
    my ($self) = @_ ;

    #Restore pointers
    my $sp = $self->{'*CurrRecStack'} ;
    $self->{'*FetchMax'} = pop @$sp  ;
    $self->{'*LastKey'}  = pop @$sp  ;
    $self->{'*LastRow'}  = pop @$sp  ;
    }

## ----------------------------------------------------------------------------
##
## MoreRecords
##

sub MoreRecords

    {
    my ($self, $ignoremax) = @_ ;

    $self -> PushCurrRec ;
    $self->{'*FetchMax'} = undef if ($ignoremax) ;

    my $more = $self -> Next () ;

    $self -> PopCurrRec ;

    return $more ;
    }


## ----------------------------------------------------------------------------
##
## PrevNextForm
##
##
##  $textprev   = Text for previous button
##  $textnext   = Text for next button
##  \%fdat      = fields/values for select where
##
##


sub PrevNextForm

    {
    my ($self, $textprev, $textnext, $fdat) = @_ ;

    my $more  = $self -> MoreRecords (1) ;
    my $start = $self -> {'*FetchStart'} ;
    my $max   = $self -> {'*FetchMax'} - $self -> {'*FetchStart'} + 1 ;

    
    my $esc = '' ;
    $esc = '\\' if (defined ($HTML::Embperl::escmode) && ($HTML::Embperl::escmode & 1)) ;
    my $buttons = "$esc<form method=$esc\"POST$esc\"$esc>$esc<input type=$esc\"hidden$esc\" name=$esc\"\$start$esc\" value=$esc\"$start$esc\"$esc>\n$esc<input type=$esc\"hidden$esc\" name=$esc\"\$max$esc\" value=$esc\"$max$esc\"$esc>\n" ;
    my $k ;
    my $v ;

    if ($fdat)
        {
        while (($k, $v) = each (%$fdat))
            {
            if (substr ($k, 0, 1) eq '\\')
        	    {
        	    $k = '\\' . $k ;
        	    }
            if ($k ne '$start' && $k ne '$max' && $k ne '$prev' && $k ne '$next')
        	    {
	            $buttons .= "$esc<input type=$esc\"hidden$esc\" name=$esc\"" . $k . "$esc\" value=$esc\"$v$esc\"$esc>\n" ;
		    }
            }
        }
    if ($start > 0 && $textprev)
        {
        $buttons .= "$esc<input type=$esc\"submit$esc\" name=$esc\"\$prev$esc\" value=$esc\"$textprev$esc\"$esc>" ;
        }
    if ($more > 0 && $textnext)
        {
        $buttons .= "$esc<input type=$esc\"submit$esc\" name=$esc\"\$next$esc\" value=$esc\"$textnext$esc\"$esc>" ;
        }
    $buttons .= "$esc</form$esc>" ;

    return $buttons ;    
    }




##########################################################################################

1;

package DBIx::Recordset::CurrRow ;


use Carp ;

## ----------------------------------------------------------------------------
##
## TIEHASH
##
## tie an hash to the object, object must be aready blessed
##
## tie %self, 'DBIx::Recordset::CurrRow', $self ;
##

sub TIEHASH
    {
    my ($class, $arg) = @_ ;
    my $rs ;    
    
    if (ref ($arg) eq 'HASH')
        {
        $rs = DBIx::Recordset -> SetupObject ($arg) or return undef ;
        }
    elsif (ref ($arg) eq 'DBIx::Recordset')
        {
        $rs = $arg ;
        }
    else
        {
        croak ("Need DBIx::Recordset or setup parameter") ;
        }


    my $self = {'*Recordset' => $rs} ;

    bless ($self, $class) ;
    
    return $self ;
    }




## ----------------------------------------------------------------------------
##
## Fetch the data from a previous SQL Select
##
## $fetch     = Column to fetch
## 
##


sub FETCH ()
    {
    if (wantarray)
        {
        my @result ;
        my $rs = $_[0] -> {'*Recordset'} ;
        $rs -> PushCurrRec ;
        my $rec = $rs -> First () ;
        while ($rec)
            {
            push @result, tied (%$rec) -> FETCH ($_[1]) ;
            $rec = $rs -> Next () ;
            }
        $rs -> PopCurrRec ;
        return @result ;
        }
    else
        {
        my $rec = $_[0] -> {'*Recordset'} -> Curr ;
        return tied (%$rec) -> FETCH ($_[1]) if (defined ($rec)) ;
        return undef ;
        }
    }


## ----------------------------------------------------------------------------

sub STORE ()
    {
    if (ref $_[2])
        { # array
        my ($self, $key, $dat) = @_ ;
        my $rs = $self -> {'*Recordset'} ;
        $rs -> PushCurrRec ;
        my $rec = $rs -> First (1) ;
        my $i = 0 ;
        while ($rec)
            {
            tied (%$rec) -> STORE ($key, $$dat[$i++]) ;
            last if ($i > $#$dat) ;
            $rec = $rs -> Next (1) ;
            }
        $rs -> PopCurrRec ;
        }
    else
        {
        tied (%{$_[0] -> {'*Recordset'} -> Curr (1)}) -> STORE ($_[1], $_[2]) ;
        }
    }


## ----------------------------------------------------------------------------

sub FIRSTKEY 
    {
    my $rec = $_[0] -> {'*Recordset'} -> Curr ;
    return tied (%{$rec}) -> FIRSTKEY ; 
    }


## ----------------------------------------------------------------------------

sub NEXTKEY 
    {
    my $rec = $_[0] -> {'*Recordset'} -> Curr ;
    return tied (%{$rec}) -> NEXTKEY ; 
    }

## ----------------------------------------------------------------------------

sub EXISTS
    {
    return exists ($_[0] -> {'*Recordset'} -> Curr -> {$_[1]}) ;
    }

## ----------------------------------------------------------------------------

sub DELETE
    {
    carp ("Cannot DELETE a field from a database record") ;
    }
                
## ----------------------------------------------------------------------------

sub CLEAR ($)

    {
    #carp ("Cannot DELETE all fields from a database record") ;
    } 

## ----------------------------------------------------------------------------

sub DESTROY

    {
    my $self = shift ;

    $self -> {'*Recordset'} -> ReleaseRecords () ;
    
    print DBIx::Recordset::LOG "DB:  ::CurrRow::DESTROY\n" if ($self -> {'*Recordset'} -> {'*Debug'} > 1) ;
    }

##########################################################################################

package DBIx::Recordset::Hash ;

use Carp ;

## ----------------------------------------------------------------------------
##
## TIEHASH
##
## tie an hash to the object, object must be aready blessed
##
## tie %self, 'DBIx::Recordset::Hash', $self ;
##

sub TIEHASH
    {
    my ($class, $arg) = @_ ;
    my $rs ;    
    
    if (ref ($arg) eq 'HASH')
        {
        $rs = DBIx::Recordset -> SetupObject ($arg) or return undef ;
        }
    elsif (ref ($arg) eq 'DBIx::Recordset')
        {
        $rs = $arg ;
        }
    else
        {
        croak ("Need DBIx::Recordset or setup parameter") ;
        }


    my $self = {'*Recordset' => $rs} ;

    bless ($self, $class) ;
    
    return $self ;
    }


## ----------------------------------------------------------------------------
##
## Fetch the data from a previous SQL Select
##
## $fetch     = PrimKey for Row to fetch
## 
##


sub FETCH 
    {
    my ($self, $fetch) = @_ ;

    my $rs    = $self->{'*Recordset'} ;  
    my $h ;

    print DBIx::Recordset::LOG "DB:  Hash::FETCH \{" . (defined ($fetch)?$fetch:'<undef>') ."\}\n"  if ($rs->{'*Debug'} > 1) ;

    if (!defined ($rs->{'*LastKey'}) || $fetch ne $rs->{'*LastKey'})
        {
        if ($rs->{'*Placeholders'})
            { $rs->SQLSelect ("$rs->{'*PrimKey'} = ?", undef, undef, undef, undef, [$fetch]) or return undef ; }
        elsif ($rs->{'*Quote'}{$rs->{'*PrimKey'}})
            { $rs->SQLSelect ("$rs->{'*PrimKey'} = " . $rs->{'*DBHdl'} -> quote ($fetch)) or return undef ; }
        else        
            { $rs->SQLSelect ("$rs->{'*PrimKey'} = " . ($fetch+0)) or return undef ; }
    
        $h = $rs -> FETCH (0) ;
        }
    else
        {
        $h = $rs -> Curr ;
        }

    print DBIx::Recordset::LOG "DB:  Hash::FETCH return " . defined ($h)?$h:'<undef>' . "\n" if ($rs->{'*Debug'} > 1) ;
    
    return $h ;
    }

## ----------------------------------------------------------------------------
##
## store something in the hash
##
## $key     = PrimKey for Row to fetch
## $value   = Hashref with row data
##

sub STORE

    {
    my ($self, $key, $value) = @_ ;
    my $rs    = $self -> {'*Recordset'} ;  

    print DBIx::Recordset::LOG "DB:  ::Hash::STORE \{$key\} = $value\n" ;# if ($rs->{'*Debug'} > 1) ;

    croak "Hash::STORE need hashref as value" if (!ref ($value) eq 'HASH') ;

    my %dat = %$value ;                 # save values, if any
    $dat{$rs -> {'*PrimKey'}} = $key ;  # setup primary key value
    %$value = () ;                      # clear out data in tied hash
    my $r = tie %$value, 'DBIx::Recordset::Row', $rs, \%dat, undef, 1 ;
    
    #$r -> STORE ($rs -> {'*PrimKey'}, $key) ;
    #$r -> {'*new'}   = 1 ;
    
    # setup recordset
    $rs-> ReleaseRecords ;
    $DBIx::Recordset::Data{$rs-> {'*Id'}}[0] = $value ;
    $rs-> {'*LastKey'} = $key ;
    $rs-> {'*CurrRow'} = 1 ;
    $rs-> {'*LastRow'} = 0 ;
    } 

## ----------------------------------------------------------------------------

sub FIRSTKEY 
    {
    my $rs    = $_[0]->{'*Recordset'} ;  

    $rs->SQLSelect () or return undef ; 

    my $dat = $rs -> First (0) or return undef ;
    my $key = $dat -> {$rs->{'*PrimKey'}} ;
    
    if ($rs->{'*Debug'} > 1) 
        {
        print DBIx::Recordset::LOG "DB:  Hash::FIRSTKEY \{" . (defined ($key)?$key:'<undef>') . "\}\n" ;
        }        

    return $key ;
    }

## ----------------------------------------------------------------------------

sub NEXTKEY 
    {
    my $rs    = $_[0]->{'*Recordset'} ;  

    my $dat   = $rs -> Next () or return undef ;
    my $key   = $dat -> {$rs->{'*PrimKey'}} ;

    if ($rs->{'*Debug'} > 1) 
        {
        print DBIx::Recordset::LOG "DB:  Hash::NEXTKEY \{" . (defined ($key)?$key:'<undef>') . "\}\n" ;
        }        

    return $key ;
    }

## ----------------------------------------------------------------------------

sub EXISTS
    {
    return defined ($_[0] -> FETCH ($_[1])) ;
    }

## ----------------------------------------------------------------------------

sub DELETE
    {
    carp ("DELETE not implemented") ;
    }
                
## ----------------------------------------------------------------------------

sub CLEAR 

    {
    carp ("CLEAR not implemented") ;
    } 

## ----------------------------------------------------------------------------

sub Flush

    {
    $_[0]->{'*Recordset'} -> Flush () ;
    }

## ----------------------------------------------------------------------------

sub DESTROY

    {
    my $self = shift ;
    
    $self -> {'*Recordset'} -> ReleaseRecords () ;

    print DBIx::Recordset::LOG "DB:  ::Hash::DESTROY\n" if ($self -> {'*Recordset'} -> {'*Debug'} > 1) ;
    }

##########################################################################################

package DBIx::Recordset::Row ;

use Carp ;

sub TIEHASH  

    {
    my ($class, $rs, $names, $dat, $new) = @_ ;

    my $self = {'*Recordset' => $rs} ;
    my $data = $self -> {'*data'} = {} ;
    my $upd  = $self -> {'*upd'}  = {} ;

    bless ($self, $class) ;
 
    if (ref ($names) eq 'HASH')
        {
        my $v ;
        my $k ;

        if ($new)
            {
            $self->{'*new'}     = 1 ;                  # mark it as new record
            $self->{'*dirty'}   = 1 ;                  # mark it as dirty
            
            
            my $lk ;
            while (($k, $v) = each (%$names))
                {
                $lk = lc ($k) ;
                # store the value and remeber it for later update
                $upd ->{$lk} = \($data->{$lk} = $v) ;
                }
            }
        else
            {
            while (($k, $v) = each (%$names))
                {
                $data -> {lc($k)} = $v ;
                }
            }
        }
    else
        {
        my $i = 0 ;
        %$data = map { lc($$names[$i++]) => $_ } @$dat if ($dat) ;
        $self -> {'*Recordset'} = $rs ; 
        }

    if (!$new)
        {
        my $pk = $rs -> {'*PrimKey'} ;

        if ($pk && exists ($data -> {$pk})) 
            {
            $self -> {'*PrimKeyOrgValue'} = $data -> {$pk} ;
            }
        else
            {
            # save whole record for usage as key in later update
            %{$self -> {'*org'}} = %$data ;

            $self -> {'*PrimKeyOrgValue'} = $self -> {'*org'} ;
            }
        }


    return $self ;
    }

## ----------------------------------------------------------------------------

sub STORE
    {
    my $rs    = $_[0] -> {'*Recordset'} ;  
    print DBIx::Recordset::LOG "DB:  Row::STORE $_[1] = $_[2]\n" if ($rs->{'*Debug'} > 1) ;
    # store the value and remeber it for later update
    $_[0]->{'*upd'}{$_[1]} = \($_[0]->{'*data'}{$_[1]} = $_[2]) ;
    $_[0]->{'*dirty'}   = 1 ;                  # mark row dirty
    }

## ----------------------------------------------------------------------------

sub FETCH
    {
    return undef if (!$_[1]) ;
    my $rs    = $_[0] -> {'*Recordset'} ;  
    my $data = $_[0]->{'*data'}{$_[1]} ;
    my $link ;
    if (!defined($data) && defined ($link = $rs -> {'*Links'}{$_[1]}))
        {
        my $lf = $link -> {'!LinkedField'} ;
        my $mv = $_[0]->{'*data'}{$link -> {'!MainField'}} ;
        $link -> {$lf} = $mv ;
        $link -> {'!Default'} = { $lf => $mv } ;
        $link -> {'!DataSource'} = $rs if (!defined ($link -> {'!DataSource'})) ;
        $data = $_[0]->{'*data'}{$_[1]} = DBIx::Recordset -> Search ($link) ;
        delete $link -> {'!Recordset'} if (ref ($link -> {'!DataSource'})) ; # avoid backlinks
        print DBIx::Recordset::LOG "DB:  Row::FETCH $_[1] = Setup New Recordset for table $link->{'!Table'}, $lf= <$mv>\n" if ($rs->{'*Debug'} > 1) ;
        }
    else
        {
        print DBIx::Recordset::LOG "DB:  Row::FETCH $_[1] = <" . $data . ">\n" if ($rs->{'*Debug'} > 1) ;
        }
    
    return $data ;
    }

## ----------------------------------------------------------------------------

sub FIRSTKEY
    {
    my ($self) = @_ ;
    my $a = scalar keys %{$self -> {'*data'}};
    
    return each %{$self -> {'*data'}} ;
    }

## ----------------------------------------------------------------------------

sub NEXTKEY
    {
    return each %{$_[0] -> {'*data'}} ;
    }

## ----------------------------------------------------------------------------

sub EXISTS
    {
    exists ($_[0]->{'*data'}{$_[1]}) ;
    }


## ----------------------------------------------------------------------------

sub DELETE
    {
    carp ("Cannot DELETE a field from a database record") ;
    }
                
## ----------------------------------------------------------------------------

sub CLEAR ($)

    {
    #carp ("Cannot DELETE all fields from a database record") ;
    } 

## ----------------------------------------------------------------------------
##
## Flush data to database if row is dirty
##


sub Flush

    {
    my $self = shift ;
    my $rs    = $self -> {'*Recordset'} ;  
    
    #print DBIx::Recordset::LOG "DB:  Row::Flush rs=$rs dirty=$self->{'*dirty'}\n" ;
    return 1 if (!$rs) ;
    if ($self -> {'*dirty'}) 
        {
        print DBIx::Recordset::LOG "DB:  Row::Flush\n" if ($rs->{'*Debug'} > 1) ;

        my $dat = $self -> {'*upd'} ;
        if ($self -> {'*new'})
            {
            $rs -> Insert ($dat) or return undef ;
            }
        else
            {
            my $pko ;
            my $pk = $rs -> {'*PrimKey'} ;
            $dat->{$pk} = \($self -> {'*data'}{$pk}) if ($pk && !exists ($dat->{$pk})) ;
            #carp ("Need primary key to update record") if (!exists($self -> {"=$pk"})) ;
            if (!exists($self -> {'*PrimKeyOrgValue'})) 
                {
                $rs -> Update ($dat)  or return undef ;
                }
            elsif (ref ($pko = $self -> {'*PrimKeyOrgValue'}) eq 'HASH')
                {
                $rs -> Update ($dat, $pko)  or return undef ;
                }
            else
                {
                $rs -> Update ($dat, {$pk => $pko} )  or return undef ;
                }
            }
        delete $self -> {'*new'} ;
        delete $self -> {'*dirty'} ;
        $self -> {'*upd'} = {} ;
        }

    my $k ;
    my $v ;
    while (($k, $v) = each (%{$rs -> {'*Links'}}))
        { # Flush linked tables
        ${$self->{'*data'}{$k}} -> Flush () if (defined ($self->{'*data'}{$k})) ;
        }

    return 1 ;
    }



## ----------------------------------------------------------------------------

sub DESTROY

    {
    my $self = shift ;
    
    print DBIx::Recordset::LOG "DB:  Row::DESTROY\n" if ($DBIx::Recordset::Debug > 1 || $self -> {'*Recordset'} -> {'*Debug'} > 1) ;

    $self -> Flush () ;
    }


################################################################################

1;
__END__

=head1 NAME

DBIx::Recordset - Perl extension for DBI recordsets

=head1 SYNOPSIS

  use DBIx::Recordset;


=head1 DESCRIPTION

B<DBIx::Recordset> is a perl module, which should make it easier to access a set
of records in a database.
It should make standard database access (select/insert/update/delete)
easier to handle (e.g. web application or scripts to enter/retrieve
data to/from a database). Special attention is made for web applications to make
it possible to handle the state-less access and to process the posted data
of formfields.
The programmer only has to supply the absolutely necessary information, the
rest is done by DBIx::Recordset.

B<DBIx::Recordset> use the DBI API to access the database, so it should work with
every database for which a DBD driver is available (see also DBIx::Compat)

Most public functions take a hash reference as parameter, which makes it simple
to supply various different arguments to the same function. The parameter hash
can also be taken from a hash containing posted formfields like those available with
CGI.pm, mod_perl, HTML::Embperl and others.

Before using a recordset it is necessary to setup an object. Of course the
setup step can be made with same function call as the first database access,
but can also be handled separately.

Most functions which setup an object returns a B<typglob>. A typglob in perl is a
object which holds pointers to all datatypes with the same name. Therefore a typglob
must always have a name and B<can't> be declared with B<my>. You can only
use it as B<global> variable or declare it with B<local>. The trick for using
a typglob is that setup functions can return an B<reference to an object>, an
B<array> and an B<hash> at the same time.

The object is used to access the object methods, the array is used to access
the records currently select in the recordset and the hash is used to access
the current record.

If you don't like the idea of using typglobs you can also setup the object,
array and hash separately, or just the ones you need.

=head1 ARGUMENTS

Since most methods take a hash reference as argument, here is first a
description of the valid arguments.

=head2 Setup Parameters

All parameters starting with an '!' are only recognised at setup time.
If you specify them in later function calls they will be ignored.

=item B<!DataSource>

Driver/DB/Host. Same as the first parameter to the DBI connect function

=item B<!Table>

Tablename, multiple tables are comma-separated

=item B<!Username>

Username. Same as the second parameter to the DBI connect function.

=item B<!Password>

Password. Same as the third parameter to the DBI connect function.

=item B<!DBIAttr>

Reference to a hash which holds the attributes for the DBI connect
function. See perldoc DBI for a detailed description.

=item B<!Fields>

Fields which should be returned by a query. If you have specified multiple
tables the fieldnames should be unique. If the names are not unique you must
specify them among with the tablename (e.g. tab1.field).


NOTE 1: Fieldnames specified with !Fields can't be overridden. If you plan
to use other fields with this object later, use $Fields instead.


NOTE 2: Because the query result is returned in a hash, there can only be
one out of multiple fields  with the same name fetched at once.
If you specify multiple fields with the same name, only one is returned
from a query. Which one this actually is, depends on the DBD driver.

NOTE 3: Some databases (e.g. mSQL) requires you to always qualify a fieldname
with a tablename if more than one table is accessed in one query.

=item B<!TabRelation>

Condition which describes the relation between the given tables.
(e.g. tab1.id = tab2.id) (See also L<!TabJoin>)

  Example

  '!Table'       => 'tab1, tab2',
  '!TabRelation' => 'tab1.id=tab2.id',
  'name'         => 'foo'

  This will generate the following SQL statement:

  SELECT * FROM tab1, tab2 WHERE name = 'foo' and tab1.id=tab2.id ;


=item B<!TabJoin>

!TabJoin gives you the possibilty to specify an B<INNER/RIGHT/LEFT JOIN> which is
used in a B<SELECT> statement. (See also L<!TabRelation>)

  Example

  '!Table'   => 'tab1, tab2',
  '!TabJoin' => 'tab1 LEFT JOIN tab2 ON	(tab1.id=tab2.id)',
  'name'     => 'foo'

  This will generate the following SQL statement:

  SELECT * FROM tab1 LEFT JOIN tab2 ON	(tab1.id=tab2.id) WHERE name = 'foo' ;



=item B<!PrimKey>

Name of primary key. If specified, DBIx::Recordset assumes, that this is a unique
key to the given table(s). DBIx::Recordset can not verify this; you are responsible
for specifying the right key. If such a primary key exists in your table, you
should specify it here, because it helps DBIx::Recordset the optimise the building
of WHERE expressions.

=item B<!StoreAll>

If present will cause DBIx::Recordset to store all rows which will be fetched between
consecutive accesses, so it's possible to access data in a random order. (e.g.
row 5, 2, 7, 1 etc.) If not specified rows will only be fetched into memory
if requested, this means you have to access rows in an ascending order.
(e.g. 1,2,3 if you try 3,2,4 you will get an undef for row 2 while 3 and 4 is ok)
see also B<DATA ACCESS> below

=item B<!HashAsRowKey>

By default the hash with is returned by the setup functions is tied to the
current record, i.e. you can use it to access the fields of the current
record. If you set this parameter to true the hash will by tied to the whole
database, this means that the key of the hash will be used as primary key in
the table to select one row. (This parameter has only an effect on functions
which return a typglob)

=item B<!IgnoreEmpty>

This parameter defines how B<empty> and B<undefined> values are handled. 
The values 1 and 2 maybe helpfull when using DBIx::Recordset inside a CGI
script, because the browser send empty formfields as empty strings.

=over 4

=item B<0 (default)>

An undefinded values is treated as SQL B<NULL>, an empty strings stays as an empty string.

=item B<1>

All fields with an undefined value are ignored when building the WHERE expression.

=item B<1>

All fields with an undefined value or an empty string are ignored when building the WHERE expression.

=back

B<NOTE:> The default for version before 0.18 was 2.



=item B<!Links>

This parameter can be used to link multiple tables together. It takes a
reference to a hash, which has as keys names for a special B<"linkfield">
and as value a paramter hash. The paramter hash can contain all the
B<Setup paramters>. The setup paramters are taken to construct a new
recordset object to access the linked table. If !DataSource is omitted (what
it should normaly), the same DataSource (and database handle), as the
main object is taken. There are special paramters which can only be
occur in a link definition (see next paragraph). For a detail description
how links are handled see "Accessing mulitple Tables" below.

=head2 Link Parameters

=item B<!MainField>

The B<!MailField> parameter holds a fieldname which is used to retrive
a key value for the search in the linked table from the main table.
If obmitted it is set to the same value as B<!LinkedField>.

=item B<!LinkedField>

The fieldname which holds the key value in the linked table.
If obmitted it is set to the same value as B<!MainField>.


=head2 Where Parameters

The following parameters are used to build a SQL WHERE expression

=item B<{fieldname}>

Value for field. The value will be automatically quoted if necessary.

=item B<'{fieldname}>

Value for field. The value will always be quoted. This is only necessary if
DBIx::Recordset cannot determine the correct type for a field.

=item B<#{fieldname}>

Value for field. The value will never be quoted, but converted a to number.
This is only necessary if
DBIx::Recordset cannot determine the correct type for a field.

=item B<\{fieldname}>

Value for field. The value will not be converted in any way i.e. you have to
quote it before supplying it to DBIx::Recordset if necessary.

=item B<+{fieldname}|{fieldname}..>

Values for multiple fields. The value must be in one/all fields depending on $compconj
 Example:
 '+name|text' => 'abc' will expand to name='abc' or text='abc'

=item B<$compconj>

'or' or 'and' (default is 'or'). Specifies the conjunction between multiple
fields. (see above)

=item B<$valuesplit>

Regular expression for splitting a field value in multiple values
(default is '\t') The conjunction for multiple values could be specified
with B<$valueconj>. Per default only one of the value must match the field.


 Example:
 'name' => "mouse\tcat" will expand to name='mouse' or name='cat'

=item B<$valueconj>

'or' or 'and' (default is 'or'). Specifies the conjunction for multiple values.

=item B<$conj>

'or' or 'and' (default is 'and') conjunction between fields

=item B<$operator>

Default operator if not otherwise specified for a field. (default is '=')

=item B<*{fieldname}>

Operator for the named field

 Example:
 'value' => 9, '*value' => '>' expand to value > 9

=head2 Search parameters

=item B<$start>

First row to fetch. The row specified here will be appear as index 0 in
the data array

=item B<$max>

Maximum number of rows to fetch. Every attempt to fetch more rows the specified
here will return undef, even if the select returns more rows.

=item B<$next>

Add the number supplied with B<$max> to B<$start>. This intended to implement
a next button.

=item B<$prev>

Subtract the number supplied with B<$max> from B<$start>. This intended to implement
a previous button.

=item B<$order>

Fieldname(s) for ordering (ORDER BY)(must be comma separated, could also contain USING)

=item B<$group>

Fieldname(s) for grouping (GROUP BY)(must be comma separated, could also contain HAVING)

=item B<$append>

String which is append at the end of an SELECT statement, can contain any data.

=item B<$fields>

Fields which should be return by a query. If you have specified multiple
tables the fieldnames should be unique. If the names are not unique you must
specify them among with the tablename (e.g. tab1.field).


NOTE 1: If B<!fields> is supplied at setup time, this can not be overridden
by $fields.

NOTE: Because the query result is returned in a hash, there can only be
one out of multiple fields  with the same name fetched at once.
If you specify multiple fields with same name, only one is returned
from a query. Which one this actually is, depends on the DBD driver.

=item B<$primkey>

Name of primary key. DBIx::Recordset assumes, that if specified this is a unique
key to the given table(s). DBIx::Recordset can not verify this, you are responsible
for specifying the right key. If such a primary exists in your table, you
should specify it here, because it helps DBIx::Recordset the optimise the building
of WHERE expressions.

See also B<!primkey>


=head2 Execute parameters

The following parameter specify which action is to be executed

=item B<=search>

search data

=item B<=update>

update record(s)

=item B<=insert>

insert record

=item B<=delete>

delete record(s)

=item B<=empty>

setup empty object


=head1 METHODS


=item B<*set = DBIx::Recordset -E<gt> Setup (\%params)>

Setup a new object and connect to to a database and table(s). Collects
information about the tables which are later needed. Returns a typglob
which can be used to access the object ($set), an array (@set) and an
hash (%set).

B<params:> setup

=item B<$set = DBIx::Recordset -E<gt> SetupObject (\%params)>

Same as above, but setup only the object, do not tie anything (no array, no hash)

B<params:> setup

=item B<$set = tie @set, 'DBIx::Recordset', $set>

=item B<$set = tie @set, 'DBIx::Recordset', \%params>

Ties an array to an recordset object. The result of an query which is executed
by the returned object can be accessed via the tied array. If array content
is modified the database updated accordingly (see Data access below for
more details)
The first form ties the array to an already existing object, the second one
setup a new object.

B<params:> setup


=item B<$set = tie %set, 'DBIx::Recordset::Hash', $set>

=item B<$set = tie %set, 'DBIx::Recordset::Hash', \%params>

Ties a hash to an recordset object. The hash can be used to access/update/insert
single rows of an table: the hash key is identical to the primary key
value of the table. (see Data access below for more details)

The first form ties the hash to an already existing object, the second one
sets up a new object.

B<params:> setup



=item B<$set = tie %set, 'DBIx::Recordset::CurrRow', $set>

=item B<$set = tie %set, 'DBIx::Recordset::CurrRow', \%params>

Ties a hash to an recordset object. The hash can be used to access the fields
of the current record of the recordset object.
(see Data access below for more details)

The first form ties the hash to an already existing object, the second one
sets up a new object.

B<params:> setup


=item B<*set = DBIx::Recordset -E<gt> Select (\%params, $fields, $order)>

=item B<$set -E<gt> Select (\%params, $fields, $order)>

=item B<$set -E<gt> Select ($where, $fields, $order)>

Selects records from the recordsets table(s)

The first syntax setups a new DBIx::Recordset object and does the select.

The second and third syntax selects into an existing DBIx::Recordset object.


B<params:> setup (only syntax 1), where  (without $order and $fields)

B<where:>  (only syntax 3) string for SQL WHERE expression

B<fields:> comma separated list of fieldnames to select

B<order:>  comma separated list of fieldnames to sort on



=item B<*set = DBIx::Recordset -E<gt> Search (\%params)>

=item B<set -E<gt> Search (\%params)>

Does a search on the given tables and makes data ready to access them via
@set or %set. First syntax also setup a new object.

B<params:> setup (only syntax 1), where, search


=item B<*set = DBIx::Recordset -E<gt> Insert (\%params)>

=item B<$set -E<gt> Insert (\%params)>

Insert a new record in the recordset table(s). Params should contain one
entry for every field you want to insert a value.

B<params:> setup (only syntax 1), fields



=item B<*set = DBIx::Recordset -E<gt> Update (\%params, $where)>

=item B<*set = DBIx::Recordset -E<gt> Update (\%params, $where)>

=item B<set -E<gt> Update (\%params, $where)>

=item B<set -E<gt> Update (\%params, $where)>

Updates one or more record in the recordset table(s). Params should contain
one entry for every field you want to update. The $where contains the SQL WHERE
condition as string or as reference to a hash. If $where is omitted the
where conditions are build from the params.

B<params:> setup (only syntax 1+2), where (only if $where is omitted), fields



=item B<*set = DBIx::Recordset -E<gt> Delete (\%params)>

=item B<$set -E<gt> Delete (\%params)>

Deletes one or more records form the recordsets table(s)

B<params:> setup (only syntax 1), where


=item B<*set = DBIx::Recordset -E<gt> Execute (\%params)>

=item B<$set -E<gt> Execute (\%params)>

Executes one of the above methods, depending on the given arguments.
If multiple execute parameter are specified the priority is
 =search
 =update
 =insert
 =delete
 =empty

If none of the above parameters are specified, a search is performed.


B<params:> setup (only syntax 1), execute, where, search, fields


=item B<$set -E<gt> do ($statement, $attribs, \%params)>

Same as DBI do. Executes a single SQL statement on the open database.


=item B<$set -E<gt> First ()>

Position the record pointer to the first row


=item B<$set -E<gt> Next ()>

Position the record pointer to the next row


=item B<$set -E<gt> Prev ()>

Position the record pointer to the previous row

=item B<$set -E<gt> AllNames ()>

Returns an reference to an array of all fieldnames of all tables
used by the object

=item B<$set -E<gt> Names ()>

Returns an reference to an array of the fieldnames from the last
query.

=item B<$set -E<gt> AllTypes ()>

Returns an reference to an array of all fieldtypes of all tables
used by the object

=item B<$set -E<gt> Types ()>

Returns an reference to an array of the fieldtypes from the last
query.

=item B<$set -E<gt> Add ()>

=item B<$set -E<gt> Add (\%data)>

Adds a new row to a recordset. The first one add an empty row, the
second one will assign initial data to it.
The Add method returns an index into the array where the new record
is located.

  Example:

  # Add an empty record
  $i = $set -> Add () ;
  # Now assign some data
  $set[$i]{id} = 5 ;
  $set[$i]{name} = 'test' ;
  # and here it is written to the database
  # (without Flush it is written, when the record goes out of scope)
  $set -> Flush () ;

Add will also set the current record to the newly created empty
record. So you can assign the data by simply using the current record.

  # Add an empty record
  $set -> Add () ;
  # Now assign some data to the new record
  $set{id} = 5 ;
  $set{name} = 'test' ;


=item B<$set -E<gt> MoreRecords ([$ignoremax])>

Returns true if there are more records to fetch from the current
recordset. If the $ignoremax parameter is specified and is true
More ignores the $max parameter of the last Search.

To tell you if there are more records More actually fetch the next
record from the database and stores it in memory, but it does not
change the current record.

=item B<$set -E<gt> PrevNextForm ($prevtext, $nexttext, \%fdat)>

Returns a HTML form which contains a previous and a next button and
all data from %fdat, as hidden fields. When callikng the Search method,
You must set the $max parameter to the number of rows
you want to see at once. After the search and the retrieval of the
rows, you can call PrevNextForm to generate the needed buttons for
scrolling thru the recordset.


=item B<$set -E<gt> Flush>

The Flush method flushes all data to the database and therefor makes sure
that the db is uptodate. Normaly DBIx::Recordset holds the updates in memory
until the row is destroyed, by either a new Select/Search or by the Recordsetobject
itself is destroyed. With this method you can make sure that every update is
really written to the db.


=item B<DBIx::Recordset::Undef ($name)>

Undef takes the name of a typglob and will destroy the array, the hash
and the object. All unwritten data is  written to the db, all
db connections are closed and all memory is freed.

  Example:
  # this destroys $set, @set and %set
  DBIx::Recordset::Undef ('set') ;



=item B<$set -E<gt> DBHdl ()>

Returns the DBI database handle


=item B<$set -E<gt> StHdl ()>

Returns the DBI statement handle of the last select


=item B<$set -E<gt> StartRecordNo ()>

Returns the record no of the record which will be returned for index 0


B<$set -E<gt> LastSQLStatement ()>

Returns the last executed SQL Statement


B<$set -E<gt> Disconnect ()>

Close the connection to the database


=head1 DATA ACCESS

The data which is returned by a B<Select> or a B<Search> can be accessed
in two ways:

1.) Through an array. Each item of the array corresponds to one of
the selected records. Each array-item is a reference to a hash containing
an entry for every field.

Example:
 $set[1]{id}	    access the field 'id' of the second record found
 $set[3]{name}	    access the field 'name' of the third record found

The record is fetched from the DBD driver when you access it the first time
and is stored by DBIx::Recordset for later access. If you don't access the records
one after each other, the skipped records are not stored and therefore can't be
accessed anymore, unless you specify the B<!StoreAll> parameter.

2.) DBIx::Recordset holds a B<currecnt record> which can be accessed directly via
a hash. The current record is the one you last access via the array. After
a Select or Search it's reset to the first record. You can change the current
record via the methods B<Next>, B<Prev>, B<First>, B<Add>.

Example:
 $set{id}	    access the field 'id' of the current record
 $set{name}	    access the field 'name' of the current record



Instead of doing a B<Select> or B<Search> you can directly access one row
of a table when you have tied n hash to DBIx::Recordset::Hash or have
specified the B<!HashAsRowKey> Parameter.
The hashkey will work as primary key to the table. You must specify the
B<!PrimKey> as setup parameter.

Example:
 $set{4}{name}	    access the field 'name' of the row with primary key = 4

=head1 MODIFYING DATA DIRECTLY

One way to update/insert data into the database is by using the Update, Insert
or Execute method of the DBIx::Recordset object. A second way is to directly
assign new values to the result of a previous Select/Search.

Example:
  # setup an new object and serach all records with name xyz
  *set = DBIx::Recordset -> Search ({'!DataSource' => 'dbi:db:tab',
				     '!PrimKey => 'id',
				     '!Table'  => 'tabname',
				     'name'    => 'xyz'}) ;

  #now you can update an existing record by assigning new values
  #Note: if possible specify a PrimKey for update to work faster
  $set[0]{'name'} = 'zyx' ;

  # or insert a new record by setting up an new array row
  $set[9]{'name'} = 'foo' ;
  $set[9]{'id'}   = 10 ;

  # if you don't know the index of a new row you can obtain
  # one by using Add
  my $i = $set -> Add () ;
  $set[$i]{'name'} = 'more foo' ;
  $set[$i]{'id'}   = 11 ;

  # or add an empty record via Add and assign the values to the current
  # reocord
  $set -> Add () ;
  $set{'name'} = 'more foo' ;
  $set{'id'}   = 11 ;

  # or insert the data directly via Add
  $set -> Add ({'name' => 'even more foo',
		'id'   => 12}) ;

  # NOTE: until here NO data is actualy written to the db!

  # we are done with that object,  Undef will flush all data to the db
  DBIx::Reocrdset::Undef ('set') ;

IMPORTANT: The data is not written to the database until you explicitly
call B<flush>, an new query is started or the object is destroyed. This will
keep the actual writes to the database to a minimum.

=head1 WORKING WITH MULTIPLE TABLES

DBIx::Recordset has some nice features to make working with multiple tables
and their relationen easier. First of all you can specify more then one
table to the B<!Table> parameter. If you do so you need to specifiy how both
tables are related. You do this with B<!TabRelation> parameter. This method
will access all the specified tables simultanouly.

=head2	Example:

If you have the following two tables, where the field streetid is an
pointer into the table street:

  table name
  name	    char (30),
  streetid  integer

  table street
  id	    integer,
  street    char (30)

You can perform the following search:

  *set = DBIx::Recordset -> Search ({'!DataSource' => 'dbi:drv:db',
		     '!Table'	   => 'name, street',
		     '!TabRelation'=> 'name.streetid = street.id'}) ;

The result is that you get a set which contains the fields B<name>, B<streetid>,
B<street> and B<id>, where id is always equal to streetid. If there are multiple
streets for one name, you will get as much records for that name as streets
present for it. For this reason this aproach works best when you have an
1:1 relation.

There is also the possibility to specifies B<JOINs>. You can do it like this

  *set = DBIx::Recordset -> Search ({
            '!DataSource' => 'dbi:drv:db',
	    '!Table'   => 'name, street',
	    '!TabJoin' => 'name LEFT JOIN street ON (name.streetid=street.id)'}) ;


The difference to the first example is, that this version returns also an record
if not both tables conatins a record for an given id. How this performed depends
on the JOIN you are given (LEFT/RIGHT/INNER) (see your SQL documentation for details
about JOINs).


If you have an 1:n relations between two tables the following may be a better
way to handle it:

  *set = DBIx::Recordset -> Search ({'!DataSource' => 'dbi:drv:db',
		     '!Table'	   => 'name',
		     '!Links'	   => {
			'-street'  => {
			    '!Table' => 'street',
			    '!LinkedField' => 'id',
			    '!MainField'   => 'streetid'
			    }
			}
		    }) ;

After that query every record will contain the fields B<name> and B<streetid>.
Additionaly there is a pseudofield named B<streetlink>, which could be
used to access another recordset object, which is the result of an query
where B<streetid = id>. Use

  $set{name} to access the name field
  $set{-street}{street} to access the first street (as long as the
				    current record of the subobject isn't
				    modified)

  $set{-street}[0]{street}	first street
  $set{-street}[1]{street}	second street
  $set{-street}[2]{street}	third street

  $set[2]{-street}[1]{street} to access the second street of the
				    third name

You can have multiple linked tables in one recordset, also you can nest
linked tables or can link a table to itself.


B<NOTE:> If you select only some fields and not all, the field which is specified by
'!MainField' must be also given in the '!Fields' or '$fields' parameter.


=head1 DEBUGING

DBIx::Recordset is able to write a logfile, so you can see what's happening
inside. There are two public variables use for this purpose:

=over 4

=item $DBIx::Recordset::Debug

Debuglevel 
 0 = off
 1 = some infos (mainly the SQL Statements)
 2 = much infos

=item DBIx::Recordset::LOG

Filehandle which is used for logging. Default is STDERR, if you running under
HTML::Embperl the default is the Embperl logfile.

=back

 Example:

    # open the log file
    open LOG, "test.log" or die "Cannot open test.log" ; 

    # assign filehandle
    *DBIx::Recordset::LOG = \*LOG ; 
    
    # set debugging level
    $DBIx::Recordset::Debug = 2 ; 

    # now you can create a new DBIx::Recordset object



=head1 SECURITY

Since on possible application of DBIx::Recordset is using in a web-server
environment, some attention should paid to security issues.

The current version of DBIx::Recordset does not have an extended security management
but some features could make your database access safer. (more security features
will come in further releases).

First of all, use the security feature of your database. Assign the web server
process as few rights as possible.

The greatest security risk is when you feed DBIx::Recordset a hash which 
contains
the formfield data posted to the web server. Somebody who knows DBIx::Recordset
can post other parameters than you expected. So the first issues is to override
all parameters which should not be posted by your script.

Example:
 *set = DBIx::Recordset -> Search ({%fdat,
				                    ('!DataSource'	=>  
"dbi:$Driver:$DB",
				                     '!Table'	=>  "$Table")}) ;

Assuming your posted form data is in %fdat. The above call will make sure
nobody from outside can override the values supplied by $Driver, $DB and
$Table.

The second approach is to presetup your objects supplying the parameters
which should not change.

Somewhere in your script startup (or at server startup time) add a setup call:

 *set = DBIx::Recordset-> setup ({'!DataSource'  =>  "dbi:$Driver:$DB",
			                        '!Table'	  =>  "$Table",
			                        '!Fields'	  =>  "a, b, c"}) ;

Later when you process a request you can write:

 $set -> Search (\%fdat) ;

This will make sure that only the database specified by $Driver, $DB, the
table specified by $Table and the Fields a, b, c can be accessed.


=head1 Compability with differnet DBD drivers

I have made a great effort to make DBIx::Recordset run with various DBD drivers.
The problem is that not all necessary information is specified via the DBI interface (yet).
So I have made the module B<DBIx::Compat> which gives information about the difference
of the various DBD drivers and underlieing database systems. Currently there are definitions for:

=item B<DBD::mSQL>

=item B<DBD::mysql>

=item B<DBD::Pg>

=item B<DBD::Solid>

=item B<DBD::ODBC>

=item B<DBD::CSV>

=item B<DBD::Oracle>

DBIx::Recordset has been tested with all those DBD drivers (on linux 2.0.32, execpt DBD::ODBC
which has been tested on Windows '95 using Access 7).


If you want to use another DBD driver with DBIx::Recordset, it may
neccesary to create
an entry for that driver. See B<perldoc DBIx::Compat> for more information.





=head1 EXAMPLES

The following show some examples of how to use DBIx::Recordset. The Examples are
from the test.pl. The examples show the DBIx::Recordset call first, followed by the
generated SQL command.


 *set = DBIx::Recordset-> setup ({'!DataSource'  =>  "dbi:$Driver:$DB",
                    			    '!Table'	  =>  "$Table"}) ;

Setup an DBIx::Recordset for driver $Driver, database $DB to access table $Table.


 $set -> Select () ;

 SELECT * form <table> ;


 $set -> Select ({'id'=>2}) ;
 is the same as
 $set1 -> Select ('id=2') ;

 SELECT * form <table> WHERE id = 2 ;


 $set -> Select ({name => "Second Name\tFirst Name"}) ;

 SELECT * from <table> WHERE name = 'Second Name' or name = 'First Name' ;


 $set1 -> Select ({value => "9991 9992\t9993",
    		       '$valuesplit' => ' |\t'}) ;

 SELECT * from <table> WHERE value = 9991 or value = 9992 or value = 9993 ;


 $set -> Select ({'+name&value' => "9992"}) ;

 SELECT * from <table> WHERE name = '9992' or value = 9992 ;


 $set -> Select ({'+name&value' => "Second Name\t9991"}) ;

 SELECT * from <table> WHERE (name = 'Second Name' or name = '9991) or
			    (value = 0 or value = 9991) ;


 $set -> Search ({id => 1,name => 'First Name',addon => 'Is'}) ;

 SELECT * from <table> WHERE id = 1 and name = 'First Name' and addon = 'Is' ;


 $set1 -> Search ({'$start'=>0,'$max'=>2, '$order'=>'id'})  or die "not ok 
($DBI::errstr)" ;

 SELECT * form <table> ORDER BY id ;
 B<Note:> Because of the B<start> and B<max> only the record 0,1 will be returned


 $set1 -> Search ({'$start'=>0,'$max'=>2, '$next'=>1, '$order'=>'id'})  or die "not ok 
($DBI::errstr)" ;

 SELECT * form <table> ORDER BY id ;
 B<Note:> Because of the B<start>, B<max> and B<next> only the record 2,3 will be 
returned


 $set1 -> Search ({'$start'=>2,'$max'=>1, '$prev'=>1, '$order'=>'id'})  or die "not ok 
($DBI::errstr)" ;

 SELECT * form <table> ORDER BY id ;
 B<Note:> Because of the B<start>, B<max> and B<prev> only the record 0,1,2 will be 
returned


 $set1 -> Search ({'$start'=>5,'$max'=>5, '$next'=>1, '$order'=>'id'})  or die "not ok 
($DBI::errstr)" ;

 SELECT * form <table> ORDER BY id ;
 B<Note:> Because of the B<start>, B<max> and B<next> only the record 5-9 will be 
returned


 *set6 = DBIx::Recordset -> Search ({  '!DataSource'   =>  "dbi:$Driver:$DB",
				                        '!Table'	    =>	"t1, t2",
				                        '!TabRelation'  =>
	"t1.value=t2.value",
                                        '!Fields'       =>  'id, name, text',
                                        'id'            =>  "2\t4" }) or die "not ok 
($DBI::errstr)" ;

 SELECT id, name, text FROM t1, t2 WHERE (id=2 or id=4) and t1.value=t2.value ;


 $set6 -> Search ({'name'            =>  "Fourth Name" }) or die "not ok 
($DBI::errstr)" ;
 SELECT id, name, text FROM t1, t2 WHERE (name = 'Fourth Name') and t1.value=t2.value 
;



 $set6 -> Search ({'id'            =>  3,
                  '$operator'     =>  '<' }) or die "not ok ($DBI::errstr)" ;

 SELECT id, name, text FROM t1, t2 WHERE (id < 3) and t1.value=t2.value ;


 $set6 -> Search ({'id'            =>  4,
                  'name'          =>  'Second Name',
                  '*id'           =>  '<',
                  '*name'         =>  '<>' }) or die "not ok ($DBI::errstr)" ;

 SELECT id, name, text FROM t1, t2 WHERE (id<4 and name <> 'Second Name') and 
t1.value=t2.value ;


 $set6 -> Search ({'id'            =>  2,
                  'name'          =>  'Fourth Name',
                  '*id'           =>  '<',
                  '*name'         =>  '=',
                  '$conj'         =>  'or' }) or die "not ok ($DBI::errstr)" ;

 SELECT id, name, text FROM t1, t2 WHERE (id<2 or name='Fourth Name') and 
t1.value=t2.value ;


 $set6 -> Search ({'+id|addon'     =>  "7\tit",
                  'name'          =>  'Fourth Name',
                  '*id'           =>  '<',
                  '*addon'        =>  '=',
                  '*name'         =>  '<>',
                  '$conj'         =>  'and' }) or die "not ok ($DBI::errstr)" ;

 SELECT id, name, text FROM t1, t2 WHERE (t1.value=t2.value) and (  ((name <> Fourth 
Name)) and (  (  id < 7  or  addon = 7)  or  (  id < 0  or  addon = 0)))


 $set6 -> Search ({'+id|addon'     =>  "6\tit",
                  'name'          =>  'Fourth Name',
                  '*id'           =>  '>',
                  '*addon'        =>  '<>',
                  '*name'         =>  '=',
                  '$compconj'     =>  'and',
                  '$conj'         =>  'or' }) or die "not ok ($DBI::errstr)" ;


 SELECT id, name, text FROM t1, t2 WHERE (t1.value=t2.value) and (  ((name = Fourth 
Name)) or (  (  id > 6 and addon <> 6)  or  (  id > 0 and addon <> 0))) ;


 *set7 = DBIx::Recordset -> Search ({  '!DataSource'   =>  "dbi:$Driver:$DB",
                                    '!Table'        =>  "t1, t2",
                                    '!TabRelation'  =>  "t1.id=t2.id",
                                    '!Fields'       =>  'name, typ'}) or die "not ok 
($DBI::errstr)" ;

 SELECT name, typ FROM t1, t2 WHERE t1.id=t2.id ;


 %h = ('id'    => 22,
      'name2' => 'sqlinsert id 22',
      'value2'=> 1022) ;


 *set9 = DBIx::Recordset -> Insert ({%h,
                                    ('!DataSource'   =>  "dbi:$Driver:$DB",
                                     '!Table'        =>  "$Table[1]")}) or die "not ok 
($DBI::errstr)" ;

 INSERT INTO <table> (id, name2, value2) VALUES (22, 'sqlinsert id 22', 1022) ;


 %h = ('id'    => 22,
      'name2' => 'sqlinsert id 22u',
      'value2'=> 2022) ;


 $set9 -> Update (\%h, 'id=22') or die "not ok ($DBI::errstr)" ;

 UPDATE <table> WHERE id=22 SET id=22, name2='sqlinsert id 22u', value2=2022 ;


 %h = ('id'    => 21,
      'name2' => 'sqlinsert id 21u',
      'value2'=> 2021) ;

 *set10 = DBIx::Recordset -> Update ({%h,
                                    ('!DataSource'   =>  "dbi:$Driver:$DB",
                                     '!Table'        =>  "$Table[1]",
                                     '!PrimKey'      =>  'id')}) or die "not ok 
($DBI::errstr)" ;

 UPDATE <table> WHERE id=21 SET name2='sqlinsert id 21u', value2=2021 ;


 %h = ('id'    => 21,
      'name2' => 'Ready for delete 21u',
      'value2'=> 202331) ;


 *set11 = DBIx::Recordset -> Delete ({%h,
                                    ('!DataSource'   =>  "dbi:$Driver:$DB",
                                     '!Table'        =>  "$Table[1]",
                                     '!PrimKey'      =>  'id')}) or die "not ok 
($DBI::errstr)" ;

 DELETE FROM <table> WHERE id = 21 ;



 *set12 = DBIx::Recordset -> Execute ({'id'  => 20,
                                   '*id' => '<',
                                   '!DataSource'   =>  "dbi:$Driver:$DB",
                                   '!Table'        =>  "$Table[1]",
                                   '!PrimKey'      =>  'id'}) or die "not ok 
($DBI::errstr)" ;

 SELECT * FROM <table> WHERE id<20 ;


 *set13 = DBIx::Recordset -> Execute ({'=search' => 'ok',
                    'name'  => 'Fourth Name',
                    '!DataSource'   =>  "dbi:$Driver:$DB",
                    '!Table'        =>  "$Table[0]",
                    '!PrimKey'      =>  'id'}) or die "not ok ($DBI::errstr)" ;

 SELECT * FROM <table>  WHERE   ((name = Fourth Name))


 $set12 -> Execute ({'=insert' => 'ok',
                    'id'     => 31,
                    'name2'  => 'insert by exec',
                    'value2'  => 3031,
 # Execute should ignore the following params, since it is already setup
                    '!DataSource'   =>  "dbi:$Driver:$DB",
                    '!Table'        =>  "quztr",
                    '!PrimKey'      =>  'id99'}) or die "not ok ($DBI::errstr)" ;

 SELECT * FROM <table> ;


 $set12 -> Execute ({'=update' => 'ok',
                    'id'     => 31,
                    'name2'  => 'update by exec'}) or die "not ok ($DBI::errstr)" ;

 UPDATE <table> SET name2=update by exec,id=31 WHERE id=31 ;


 $set12 -> Execute ({'=insert' => 'ok',
                    'id'     => 32,
                    'name2'  => 'insert/upd by exec',
                    'value2'  => 3032}) or die "not ok ($DBI::errstr)" ;


 INSERT INTO <table> (name2,id,value2) VALUES (insert/upd by exec,32,3032) ;


 $set12 -> Execute ({'=delete' => 'ok',
                    'id'     => 32,
                    'name2'  => 'ins/update by exec',
                    'value2'  => 3032}) or die "not ok ($DBI::errstr)" ;

 DELETE FROM <table> WHERE id=32 ;


=head1 SUPPORT

As far as possible for me support will be available via the
DBI Users mailing list. (dbi-user@fugue.com)

=head1 AUTHOR

G.Richter (richter@dev.ecos.de)

=head1 SEE ALSO

=item perl(1)
=item DBI(3)
=item DBIx::Compat(3)
=item HTML::Embperl(3) 
http://perl.apache.org/embperl/
=item Tie::DBI(3)
http://stein.cshl.org/~lstein/Tie-DBI/


=cut

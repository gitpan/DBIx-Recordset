
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


package DBIx::Database ;

use strict 'vars' ;

use Carp ;


## ----------------------------------------------------------------------------
##
## new
##
## creates a new DBIx::Database object. This object fetches all necessary
## meta information from the database for later use by DBIx::Recordset objects.
## Also it builds a list of links between the tables.
##
##
## $data_source  = Driver/DB/Host
## $username     = Username (optional)
## $password     = Password (optional) 
## \%attr        = Attributes (optional) 
## $saveas       = Name for this DBIx::Database object to save
##                 The name can be used in Get, or as !DataSource for DBIx::Recordset
## $keepopen     = keep connection open to use in further DBIx::Recordset setups
##

sub new

    {
    my ($class, $data_source, $username, $password, $attr, $saveas, $keepopen) = @_ ;
    
    my $metakey  ;
    my $self ;

    if (!($data_source =~ /^dbi:/i)) 
        {
        $metakey    = "-DATABASE//$1"  ;
        return $DBIx::Recordset::Metadata{$metakey} ;
        }
    
    if ($saveas)
        {
        $metakey    = "-DATABASE//$saveas"  ;
        return $self if (defined ($self = $DBIx::Recordset::Metadata{$metakey})) ;
        }


    $self = {
                '*Debug'      => $DBIx::Recordset::Debug,
                '*DataSource' => $data_source,
                '*DBIAttr'    => $attr,
                '*Username'   => $username, 
               } ;

    bless ($self, $class) ;

    my $hdl ;

    if (!defined ($self->{'*DBHdl'}))
        {
        $hdl = $self->{'*DBHdl'}  = DBI->connect($self->{'*DataSource'}, $self->{'*Username'}, $password, $self->{'*DBIAttr'}) or croak "Cannot connect to $data_source" ;

        $self->{'*MainHdl'}    = 1 ;
        $self->{'*Driver'}     = $hdl->{Driver}->{Name} ;
	if ($self->{'*Driver'} eq 'Proxy')
	    {
            $self->{'*DataSource'} =~ /dsn\s*=\s*dbi:(.*?):/i ;
	    $self->{'*Driver'} = $1 ;
	    print LOG "DB:  Found DBD::Proxy, take compability entrys for driver $self->{'*Driver'}\n" if ($self->{'*Debug'} > 1) ;
	    }

        print DBIx::Recordset::LOG "DB:  Successfull connect to $self->{'*DataSource'} \n" if ($self->{'*Debug'} > 1) ;
        }
    else
        {
        $hdl = $self->{'*DBHdl'} ;
        print DBIx::Recordset::LOG "DB:  Use already open dbh for $self->{'*DataSource'}\n" if ($self->{'*Debug'} > 1) ;
        }
            
    $DBIx::Recordset::Metadata{"$self->{'*DataSource'}//*"} ||= {} ; # make sure default table is defined

    my $drv        = $self->{'*Driver'} ;
    my $metakeydsn = "$self->{'*DataSource'}//-" ;
    my $metadsn    = $DBIx::Recordset::Metadata{$metakeydsn} || {} ;
    my $tables     = $metadsn -> {'*Tables'} ;

    if (!$tables)
        { # Query the driver, which tables are available
        my $ListTables = DBIx::Compat::GetItem ($drv, 'ListTables') ;

        
        if ($ListTables)
	    {
	    my @tabs = &{$ListTables}($hdl) or croak "Cannot list tables for $self->{'*DataSource'} ($DBI::errstr)" ;
        
	    %$tables = map { $_ => 1 } @tabs ; 
	    $metadsn -> {'*Tables'} = $tables ;
	    if ($self->{'*Debug'} > 2) 
		{
		my $t ;
		foreach $t (@tabs)
		    { print DBIx::Recordset::LOG "DB:  Found table $t\n" ; }
		}
	    }
	else    
	    {
	    $metadsn -> {'*Tables'} = {} ;
	    }
            
        $DBIx::Recordset::Metadata{$metakeydsn} = $metadsn ;
        }

    my $tab ;
    my $x ;

    while (($tab, $x) = each (%{$tables}))
        {
        DBIx::Recordset::QueryMetaData ($self, $tab) ;
        }

    
    $DBIx::Recordset::Metadata{$metakey} = $self if ($metakey) ;

    # disconnect in case we are running in a Apache/mod_perl startup file
    
    if (defined ($self->{'*DBHdl'}) && !$keepopen)
        {
        $self->{'*DBHdl'} -> disconnect () ;
        undef $self->{'*DBHdl'} ;
        }
    
    return $self ;
    }

## ----------------------------------------------------------------------------
##
## Get
##
## $name = Name of DBIx::Database obecjt you what to get
##

sub Get

    {
    my ($class, $saveas) = @_ ;
    
    my $metakey  ;
    
    $metakey    = "-DATABASE//$saveas"  ;
    return $DBIx::Recordset::Metadata{$metakey} ;
    }


## ----------------------------------------------------------------------------
##
## TableAttr
##
## get and/or set and attribute for an specfic table
##
## $table = Name of table(s)
## $key   = key
## $value = value
##

sub TableAttr

    {
    my ($self, $table, $key, $value) = @_ ;

    my $meta ;
    my $metakey    = "$self->{'*DataSource'}//$table" ;
    
    if (!defined ($meta = $DBIx::Recordset::Metadata{$metakey})) 
        {
        croak "Unknow table $table in $self->{'*DataSource'}" ;
        }

    # set new value if wanted
    return $meta -> {$key} = $value if (defined ($value)) ;

    # only return value
    return $meta -> {$key} if (exists ($meta -> {$key})) ;

    # check if there is a default value
    $metakey    = "$self->{'*DataSource'}//*" ;
    
    return undef if (!defined ($meta = $DBIx::Recordset::Metadata{$metakey})) ;

    return $meta -> {$key} ;
    }


## ----------------------------------------------------------------------------
##
## TableLink
##
## get and/or set an link description for an table
##
## $table = Name of table(s)
## $key   = linkname
## $value = ref to hash with link description
##


sub TableLink

    {
    my ($self, $table, $key, $value) = @_ ;

    my $meta ;
    my $metakey    = "$self->{'*DataSource'}//$table" ;
    
    if (!defined ($meta = $DBIx::Recordset::Metadata{$metakey})) 
        {
        croak "Unknow table $table in $self->{'*DataSource'}" ;
        }

    return $meta -> {'*Links'} -> {$key} = $value if (defined ($value)) ;

    return $meta -> {'*Links'} -> {$key}  ;
    }



## ----------------------------------------------------------------------------
##
## AllTables
##
## return reference to hash which keys contains all tables of that datasource
##

sub AllTables

    {
    my $self = shift ;
    my $metakeydsn = "$self->{'*DataSource'}//-" ;
    my $metadsn    = $DBIx::Recordset::Metadata{$metakeydsn} || {} ;
    return $metadsn -> {'*Tables'} ;
    }

## ----------------------------------------------------------------------------
##
## DESTROY
##
## do cleanup
##


sub DESTROY

    {
    my $self = shift ;

    if (defined ($self->{'*DBHdl'}))
        {
        $self->{'*DBHdl'} -> disconnect () ;
        undef $self->{'*DBHdl'} ;
        }
    }


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

    %unaryoperators
    );

use DBI ;

require Exporter;

@ISA       = qw(Exporter);

$VERSION = '0.19-beta';


$id = 1 ;
$numOpen = 0 ;

$Debug = 0 ;     # Disable debugging output

use constant wmNONE   => 0 ;
use constant wmINSERT => 1 ;
use constant wmUPDATE => 2 ;
use constant wmDELETE => 4 ;
use constant wmCLEAR  => 8 ;
use constant wmALL    => 15 ;

%unaryoperators = (
    'is null' => 1,
    'is not null' => 1
	) ;


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
## QueryMetaData
##
## $table        = table (multiple tables must be comma separated)
##


sub QueryMetaData($$)

    {
    my ($self, $table) = @_ ;
            
    my $meta ;
    my $metakey    = "$self->{'*DataSource'}//$table" ;
    
    if (defined ($meta = $Metadata{$metakey})) 
        {
        print LOG "DB:   use cached meta data for $table\n" if ($self->{'*Debug'} > 2) ;
        return $meta 
        }

    my $hdl = $self->{'*DBHdl'} ;
    my $drv = $self->{'*Driver'} ;
    my $sth ;
    
    my $ListFields = DBIx::Compat::GetItem ($drv, 'ListFields') ;
    my $QuoteTypes = DBIx::Compat::GetItem ($drv, 'QuoteTypes') ;
    my $HaveTypes  = DBIx::Compat::GetItem ($drv, 'HaveTypes') ;
    my @tabs = split (/\s*\,\s*/, $table) ;
    my $tab ;
    my $ltab ;
    my %Quote ;
    my @Names ;
    my @Types ;
    my @FullNames ;
    my %Table4Field ;

    foreach $tab (@tabs)
        {
        $sth = &{$ListFields}($hdl, $tab) or croak "Cannot list fields for $tab" ;
	$ltab = lc($tab) ;
	
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
    
        push @Names, map { lc($_)} @{ $fields } ;
        push @Types, map { lc($_)} @{ $types } ;
	my $lfield ;
        foreach (@$fields)
            {
            $lfield = lc($_) ;
	    $Table4Field{$lfield} = $ltab ;        
            $Table4Field{"$ltab.$_"} = $ltab ;
            push @FullNames, "$ltab.$lfield"  ;
            }        

        $sth -> finish ;

        # Set up a hash which tells us which fields to quote and which not
        # We setup two versions, one with tablename and one without
        my $col ;
        my $fieldname ;
        for ($col = 0; $col < $num; $col++ )
            {
            if ($self->{'*Debug'} > 2)
                {
                my $n = $$fields[$col] ;
                my $t = $$types[$col] ;
                print LOG "DB: TAB = $tab, COL = $col, NAME = $n, TYPE = $t" ;
                }
            $fieldname = lc($$fields[$col]) ;
            if ($$QuoteTypes{$$types[$col]})
                {
                print LOG " -> quote\n" if ($self->{'*Debug'} > 2) ;
                $Quote {lc("$tab.$fieldname")} = 1 ;
                $Quote {lc("$fieldname")} = 1 ;
                }
            else
                {
                print LOG "\n" if ($self->{'*Debug'} > 2) ;
                $Quote {lc("$tab.$fieldname")} = 0 ;
                $Quote {lc("$fieldname")} = 0 ;
                }
            }
        print LOG "No Fields found for $tab\n" if ($num == 0 && $self->{'*Debug'} > 1) ;
        }

    print LOG "No Tables specified\n" if ($#tabs < 0 && $self->{'*Debug'} > 1) ;


    $meta = {} ;
    $meta->{'*Table4Field'}  = \%Table4Field ;
    $meta->{'*FullNames'}    = \@FullNames ;
    $meta->{'*Names'}  = \@Names ;
    $meta->{'*Types'}  = \@Types ;
    $meta->{'*Quote'}  = \%Quote ;    

    $Metadata{$metakey} = $meta ;
    

    if (!exists ($meta -> {'*Links'}))
        { 
        my $ltab ;
        my $lfield ;
        my $metakey ;
        my $subnames ;
        my $n ;

        $meta -> {'*Links'} = {} ;

        my $metakeydsn = "$self->{'*DataSource'}//-" ;
        my $metadsn    = $Metadata{$metakeydsn} || {} ;
        my $tables     = $metadsn -> {'*Tables'} ;

        if (!$tables)
            { # Query the driver, which tables are available
            my $ListTables = DBIx::Compat::GetItem ($drv, 'ListTables') ;

	    if ($ListTables)
		{            
		my @tabs = &{$ListTables}($hdl) or croak "Cannot list tables for $self->{'*DataSource'} ($DBI::errstr)" ;
		my @stab ;

		%$tables = map { @stab = split (/\./); lc($stab[$#stab]) => $_ } @tabs ; 
		$metadsn -> {'*Tables'} = $tables ;
		if ($self->{'*Debug'} > 3) 
		    {
		    my $t ;
		    foreach $t (keys %$tables)
			{ print LOG "DB:  Found table $t => $tables->{$t}\n" ; }
		    }
		}
	    else
		{
		$metadsn -> {'*Tables'} = {} ;
		}
            
            $Metadata{$metakeydsn} = $metadsn ;
            }

	if ($#tabs <= 0)
	    {
	    my $fullname ;
	    foreach $fullname (@FullNames)
		{
		my ($ntab, $n) = split (/\./, $fullname) ;
		my $prefix = '' ;

		if ($n =~ /^(.*?)__(.*?)$/)
		    {
		    $prefix = "$1__" ;
		    $n = $2 ;
		    }

		my @part = split (/_/, $n) ;
		for (my $i = 0; $i < $#part; $i++)
		    {
		    $ltab   = join ('_', @part[0..$i]) ;
		    $lfield = join ('_', @part[$i + 1..$#part]) ;
            
		    if ($tables -> {$ltab}) 
			{
			$metakey = DBIx::Recordset::QueryMetaData ($self, $ltab) ;
			$subnames = $metakey -> {'*Names'} ;
			if (grep (/^$lfield$/i, @$subnames))
			    { # setup link
			    $meta -> {'*Links'}{"-$prefix$ltab"} = {'!Table' => $ltab, '!LinkedField' => $lfield, '!MainField' => "$prefix$n", '!MainTable' => $ntab} ;
			    print LOG "Link found for $ntab.$prefix$n to $ltab.$lfield\n" if ($self->{'*Debug'} > 2) ;
                        
			    #my $metakeyby    = "$self->{'*DataSource'}//$ltab" ;
			    #my $linkedby = $Metadata{$metakeyby} -> {'*Links'} ;
			    my $linkedby = $metakey -> {'*Links'} ;
			    $linkedby -> {"-$table"} = {'!Table' => $ntab, '!MainField' => $lfield, '!LinkedField' => "$prefix$n", '!LinkedBy' => $ltab, '!MainTable' => $ltab} ;
			    }
			last ;
			}
		    }
		}
	    }
    	else
	    { 
	    foreach $ltab (@tabs)
		{
                $metakey = DBIx::Recordset::QueryMetaData ($self, $ltab) ;

		my $k ;
		my $v ;
		my $lbtab ;
		my $links = $metakey -> {'*Links'} ;
		while (($k, $v) = each (%$links))
		    {
		    if (!$meta -> {'*Links'}{$k}) 
			{
			$meta -> {'*Links'}{$k} = { %$v } ;
    			print LOG "Link copied: $k\n" if ($self->{'*Debug'} > 2) ;
			}
		    
		    }
		}
	    }

	}


    return $meta ;
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
    my ($self, $data_source,  $table, $username, $password, $attr, $autolink) = @_ ;

    $self->{'*Table'}      = $table ;
    $self->{'*Id'}         = $id++ ;

    if (!($data_source =~ /^dbi\:/i)) 
        {
        my $metakey    = "-DATABASE//$data_source"  ;
        $data_source = $DBIx::Recordset::Metadata{$metakey} if (exists $DBIx::Recordset::Metadata{$metakey}) ;
        }

    if (ref ($data_source) eq 'DBIx::Recordset')
        { # copy from another recordset
        $self->{'*Driver'}     = $data_source->{'*Driver'} ;   
        $self->{'*DataSource'} = $data_source->{'*DataSource'} ;
        $self->{'*Username'}   = $data_source->{'*Username'} ; 
        $self->{'*DBHdl'}      = $data_source->{'*DBHdl'} ;    
        $self->{'*DBIAttr'}    = $data_source->{'*DBIAttr'} ;
        $self->{'*MainHdl'}    = 0 ;
        }
    elsif (ref ($data_source) eq 'DBIx::Database')
        { # copy from database object
        $self->{'*DataSource'} = $data_source->{'*DataSource'} ;
        $self->{'*Username'}   = $data_source->{'*Username'} ; 
        $self->{'*DBIAttr'}    = $data_source->{'*DBIAttr'} ;
        $self->{'*DBHdl'}      = undef ;
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
	if ($self->{'*Driver'} eq 'Proxy')
	    {
            $self->{'*DataSource'} =~ /dsn\s*=\s*dbi:(.*?):/i ;
	    $self->{'*Driver'} = $1 ;
	    print LOG "DB:  Found DBD::Proxy, take compability entrys for driver $self->{'*Driver'}\n" if ($self->{'*Debug'} > 1) ;
	    }

        $numOpen++ ;

        print LOG "DB:  Successfull connect to $self->{'*DataSource'} (id=$self->{'*Id'}, numOpen = $numOpen)\n" if ($self->{'*Debug'} > 1) ;
        }
    else
        {
        $hdl = $self->{'*DBHdl'} ;
        print LOG "DB:  Use already open dbh for $self->{'*DataSource'} (id=$self->{'*Id'}, numOpen = $numOpen)\n" if ($self->{'*Debug'} > 1) ;
        }
            
    

    my $meta = $self -> QueryMetaData ($self->{'*Table'}) ;
    my $metakey    = "$self->{'*DataSource'}//$self->{'*Table'}" ;
    
    $self->{'*NullOperator'} = DBIx::Compat::GetItem ($self->{'*Driver'}, 'NullOperator') ;

    $meta or croak "No meta data available for $self->{'*Table'}" ;

    $self->{'*Table4Field'} = $meta->{'*Table4Field'} ;
    #$self->{'*MainFields'} = $meta->{'*MainFields'} ;
    $self->{'*FullNames'}= $meta->{'*FullNames'} ;
    $self->{'*Names'}    = $meta->{'*Names'} ;
    $self->{'*Types'}    = $meta->{'*Types'} ;
    $self->{'*Quote'}    = $meta->{'*Quote'} ;
    $self->{'*Links'}    = $meta->{'*Links'} ;
    $self->{'*PrimKey'}  = $meta->{'!PrimKey'} ;


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

    if ($self->{'*Debug'} > 0)
        {
        print LOG "DB:  ERROR open DB $data_source ($DBI::errstr)\n" if (!defined ($rc)) ;

        my $n = '' ;
        $n = ' NOT' if (!$self->{'*Placeholders'}) ;
        print LOG "DB:  New Recordset driver=$self->{'*Driver'}  placeholders$n supported\n" if ($self->{'*Debug'} > 2)
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
## !Order	= order for any query
## !TabRelation = condition which describes the relation
##                between the given tables
## !TabJoin     = JOIN to use in table part of select statement
## !PrimKey     = name of primary key
## !StoreAll	= store all fetched data
## !LinkName    = query !NameField field(s) instead of !MainField for links
##		    0 = off
##		    1 = select additional fields
##		    2 = build name in uppercase of !MainField
##		    3 = replace !MainField with content of !NameField
##
## !Default     = hash with default record data
## !IgnoreEmpty = 1 ignore undef values, 2 ignore empty strings
##
## !WriteMode   = 1 => allow insert (wmINSERT)
##                2 => allow update (wmUPDATE)
##		  4 => allow delete (wmDELETE)
##                8 => allow delete all (wmCLEAR)
##		    default = 7

sub SetupObject

    {
    my ($class, $parm) = @_ ;

    my $self = New ($class, $$parm{'!DataSource'}, $$parm{'!Table'}, $$parm{'!Username'}, $$parm{'!Password'}, $$parm{'!DBIAttr'}) or return undef ; 

    $self->{'*Fields'}      = $$parm{'!Fields'} ;
    $self->{'*TabRelation'} = $$parm{'!TabRelation'} ;
    $self->{'*TabJoin'}     = $$parm{'!TabJoin'} ;
    $self->{'*PrimKey'}     = $$parm{'!PrimKey'} if (defined ($$parm{'!PrimKey'})) ;
    $self->{'*StoreAll'}    = $$parm{'!StoreAll'} ;
    $self->{'*Default'}     = $$parm{'!Default'} if (defined ($$parm{'!Default'})) ;
    $self->{'*IgnoreEmpty'} = $$parm{'!IgnoreEmpty'} || 0 ;
    $self->{'*WriteMode'}   = $$parm{'!WriteMode'} || 7 ;
    $self->{'*LongNames'}   = $$parm{'!LongNames'} || 0 ;
    $self->{'*LinkName'}    = $$parm{'!LinkName'} if (exists $$parm{'!LinkName'}) ;
    $self->{'*LinkName'}  ||= 0 ;
    $self->{'*NameField'}   = $$parm{'!NameField'} if (exists $$parm{'!NameField'}) ;
    $Data{$self->{'*Id'}}   = [] ;
    $self->{'*FetchStart'}  = 0 ;
    $self->{'*FetchMax'}    = undef ;
    $self->{'*EOD'}         = undef ;
    $self->{'*CurrRow'}     = 0 ;
    $self->{'*MainTable'}   = lc ($self->{'*Table'}) ;
    $self->{'*Stats'} = {} ;
    $self->{'*Order'}       = $self -> TableAttr ('!Order') if ($self -> TableAttr ('!Order')) ;
    $self->{'*Order'}       = $$parm{'!Order'} if (exists $$parm{'!Order'}) ;

    my $ofunc = $self->{'*OutputFunctions'} = {} ;
    my $ifunc = $self->{'*InputFunctions'}  = {} ;
    my $names = $self->{'*Names'} ;
    my $types = $self->{'*Types'} ;
    my $key ;
    my $value ;
    my $conversion ;

    foreach $conversion (($self -> TableAttr ('!Filter'), $$parm{'!Filter'}))  
	{
	if ($conversion)
	    {
	    while (($key, $value) = each (%$conversion))
		{
		if ($key =~ /^\d*$/)
		    { # numeric -> SQL_TYPE
		    my $i = 0 ;
		    my $name ;
		    foreach (@$types)
			{
			if ($_ == $key) 
			    {
			    $name = $names -> [$i] ;
			    $ifunc -> {$name} = $value -> [0] if ($value -> [0]) ;
			    $ofunc -> {$name} = $value -> [1] if ($value -> [1]) ;
			    }
			$i++ ;
			}
		    }
		else
		    {    	    
    		    $ifunc -> {$key} = $value -> [0] if ($value -> [0]) ;
    		    $ofunc -> {$key} = $value -> [1] if ($value -> [1]) ;
		    }
		}
	    }
	}

    delete $self->{'*OutputFunctions'} if (keys (%$ofunc) == 0) ;
    	

    my $links =  $$parm{'!Links'} ;
    if (defined ($links))
        {
        my $k ;
        my $v ;
        while (($k, $v) = each (%$links))
            {
            $v -> {'!LinkedField'} = $v -> {'!MainField'} if (defined ($v) && !defined ($v -> {'!LinkedField'})) ;
            $v -> {'!MainField'}   = $v -> {'!LinkedField'} if (defined ($v) && !defined ($v -> {'!MainField'})) ;
            }
        $self->{'*Links'} = $links ;
        }

    if ($self->{'*LinkName'})
        {
        ($self->{'*Fields'}, $self->{'*Table'}, $self->{'*TabJoin'}, $self->{'*TabRelation'}, $self->{'*ReplaceFields'}) = 
               $self -> BuildFields ($self->{'*Fields'}, $self->{'*Table'}, $self->{'*TabRelation'}) ;
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
    #delete $Data{$_[0] -> {'*Id'}}  ;
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
    
    print LOG "DB:  Undef $objname\n" if (defined (${$objname}) && (${$objname}->{'*Debug'} > 1 || $Debug > 1)) ; 
    
    
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


    print LOG "DB:  Disconnect (id=$self->{'*Id'}, numOpen = $numOpen)\n" if ($self->{'*Debug'} > 1) ;
    }


## ----------------------------------------------------------------------------
##
## do some cleanup 
##

sub DESTROY ($)
    {
    my ($self) = @_ ;

    $self -> Disconnect () ;

    delete $Data{$self -> {'*Id'}}  ;

	{
	local $^W = 0 ;
	print LOG "DB:  DESTROY (id=$self->{'*Id'}, numOpen = $numOpen)\n" if ($self->{'*Debug'} > 2) ;
	}
    }



## ----------------------------------------------------------------------------
##
## begin transaction
##

sub Begin 

    {
    my ($self) = @_ ;

    $self->{'*DBHdl'} -> begin ;
    }

## ----------------------------------------------------------------------------
##
## commit transaction
##

sub Commit 

    {
    my ($self) = @_ ;

    $self -> Flush ;
    $self->{'*DBHdl'} -> commit ;
    }

## ----------------------------------------------------------------------------
##
## rollback transaction
##

sub Rollback

    {
    my ($self) = @_ ;

    $self -> ReleaseRecords ;

    $self->{'*DBHdl'} -> rollback ;
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
    print LOG "DB:  STORE \[$fetch\] = $value\n"  if ($self->{'*Debug'} > 3) ;
    if ($self->{'*Debug'} > 2 && ref ($value) eq 'HASH')
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
	my $dirty = $r->{'*dirty'} ; # preserve dirty state  
        %$value = %{$self -> {'*Default'}} if (exists ($self -> {'*Default'})) ;
	$r->{'*dirty'}   = $dirty
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
    
    print LOG "DB:  do $statement <@params>\n" if ($self->{'*Debug'} > 1) ;
    
    $self -> {'*LastSQLStatement'} = $statement ;

    my $ret = $self->{'*DBHdl'} -> do ($statement, $attribs, @params) ;

    print LOG "DB:  do returned $ret\n" if ($self->{'*Debug'} > 2) ;
    print LOG "DB:  ERROR $DBI::errstr\n"  if (!$ret && $self->{'*Debug'}) ;
    print LOG "DB:  in do $statement <@params>\n" if (!$ret && $self->{'*Debug'} == 1) ;
    
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
    my $self = shift ;
    my $sth = $self -> {'*StHdl'} ;
    return undef if (!$sth) ;
    if ($self -> {'*LinkName'} < 2)
        {
        return $self->{'*SelectFields'} ;
        }
    else
        {
        my $names = $self->{'*SelectFields'};
        my $repl = $self -> {'*ReplaceFields'} ;
        my @newnames  ;
        my $i  ;
        for ($i = 0; $i <= $#$repl; $i++)
            {
            #print LOG "### Names $i = $names->[$i]\n" ;
            push @newnames, lc($names -> [$i]) ; 
            }
        return \@newnames ;
        }
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
## Link
##
## if linkname if undef returns reference to an hash of all links
## else returns reference to that link
##

sub Link

    {
    my ($self, $linkname) = @_ ;

    my $links = $self -> {'*Links'} ;
    return undef if (!defined ($links)) ;
    return $links if (!defined ($linkname)) ;
    return $links -> {$linkname}  ;
    }

## ----------------------------------------------------------------------------
##
## Link4Field
##
## returns the Linkname for that field, if any
##

sub Link4Field

    {
    my ($self, $field) = @_ ;

    my $links = $self -> {'*Links'} ;
    return undef if (!defined ($field)) ;

    my $tab4f = $self -> {'*Table4Field'} ;

    if (!exists ($self -> {'*MainFields'}))
        {
        my $k ;
        my $v ;

        my $mf = {} ;
        my $f ;
        while (($k, $v) = each (%$links))
            {
            $f = $v -> {'!MainField'} ;
            $mf -> {$f} = $k ;
            $mf -> {"$tab4f->{$f}.$f"} = $k ;
            print LOG "DB:  Field $v->{'!MainField'} has link $k\n" ;
            }
        $self -> {'*MainFields'} = $mf ;
        }

    return $self -> {'*MainFields'} -> {$field} ;
    }

## ----------------------------------------------------------------------------
##
## Links
##
## return reference to an hash of links
##

sub Links

    {
    return $_[0] -> {'*Links'} ;
    }

## ----------------------------------------------------------------------------
##
## TableAttr
##
## get and/or set an unser defined attribute of that table
##
## $key   = key
## $value = new value (optional)
## $table = Name of table(s) (optional)
##

sub TableAttr

    {
    my ($self, $key, $value, $table) = @_ ;

   $table ||= $self -> {'*MainTable'} ;

    my $meta ;
    my $metakey    = "$self->{'*DataSource'}//$table" ;
    
    if (!defined ($meta = $DBIx::Recordset::Metadata{$metakey})) 
        {
        croak "Unknow table $table in $self->{'*DataSource'}" ;
        }

    # set new value if wanted
    return $meta -> {$key} = $value if (defined ($value)) ;

    # only return value
    return $meta -> {$key} if (exists ($meta -> {$key})) ;

    # check if there is a default value
    $metakey    = "$self->{'*DataSource'}//*" ;
    
    return undef if (!defined ($meta = $DBIx::Recordset::Metadata{$metakey})) ;

    return $meta -> {$key} ;
    }

## ----------------------------------------------------------------------------
##
## Stats
##
## return statistics
##

sub Stats

    {
    return $_[0] -> {'*Stats'} ;
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
  
    croak "Insert disabled for table $self->{'*Table'}" if (!($self->{'*WriteMode'} & wmINSERT)) ;
      
    $self->{'*Stats'}{insert}++ ;

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
    
    croak "Update disabled for table $self->{'*Table'}" if (!($self->{'*WriteMode'} & wmUPDATE)) ;

    $self->{'*Stats'}{update}++ ;

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
    
    croak "Delete disabled for table $self->{'*Table'}" if (!($self->{'*WriteMode'} & wmDELETE)) ;
    croak "Clear (Delete all) disabled for table $self->{'*Table'}" if (!$where && !($self->{'*WriteMode'} & wmCLEAR)) ;

    $self->{'*Stats'}{delete}++ ;

    return $self->do ("DELETE FROM $self->{'*Table'} " . ($where?"WHERE $where":''), undef, @$bind_values) ;
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
    $self->{'*SelectFields'} = undef ;
    $self->{'*LastRecord'} = undef ;

    $order  ||= '' ;
    $expr   ||= '' ;
    $orderby  = $order?'ORDER BY':'' ;
    $groupby  = $group?'GROUP BY':'' ;
    $where    = $expr?'WHERE':'' ;
    $fields ||= '*';
    $table    = $self->{'*TabJoin'} || $self->{'*Table'} ;

    my $statement = "SELECT $fields FROM $table $where $expr $groupby $group $orderby $order $append" ;

    if ($self->{'*Debug'} > 1)
        { 
        my $b = $bind_values || [] ;
        print LOG "DB:  $statement <@$b>\n" ;
        }

    $self -> {'*LastSQLStatement'} = $statement ;

    $self->{'*Stats'}{select}++ ;

    $sth = $self->{'*DBHdl'} -> prepare ($statement) ;

    if (defined ($sth))
        {
        $rc = $sth -> execute (@$bind_values) ;
	}
        
    
    my $names ;
    if ($rc)
    	{
	$names = $sth -> FETCH ('NAME') ;
    	$self->{'*NumFields'} = $#{$names} + 1 ;
	}
    else
    	{
	print LOG "DB:  ERROR $DBI::errstr\n"  if ($self->{'*Debug'}) ;
	print LOG "DB:  in $statement <@$bind_values>\n" if ($self->{'*Debug'} == 1) ;
    
    	$self->{'*NumFields'} = 0 ;
	
	undef $sth ;
	}

    $self->{'*CurrRow'} = 0 ;
    $self->{'*LastRow'} = 0 ;
    $self->{'*StHdl'}   = $sth ;

    my @ofunca  ;
    my $ofunc  = $self -> {'*OutputFunctions'} ;

    if ($ofunc && $names)
	{
	my $i = 0 ;

	foreach (@$names)
	    {
	    $ofunca [$i++] = $ofunc -> {lc ($_)} ;
	    }
	}

    $self -> {'*OutputFuncArray'} = \@ofunca ;
    

	
    if ($self->{'*LongNames'} && $fields eq '*')
	{
	$self->{'*SelectFields'} = $self->{'*FullNames'} ;
	}
    else
	{
	$self->{'*SelectFields'} = $names ;
	}


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

    print LOG "DB:  FETCH \[$fetch\]\n"  if ($self->{'*Debug'} > 3) ;

    $fetch += $self->{'*FetchStart'} ;

    return $self->{'*LastRecord'} if ($fetch == $self->{'*LastRecordFetch'} && $self->{'*LastRecord'}) ; 

    my $max ;
    my $key ;
    my $dat ;                           # row data

    
    $max    = $self->{'*FetchMax'} ;

    my $row = $self->{'*CurrRow'} ;     # row next to fetch from db
    my $sth = $self->{'*StHdl'} ;       # statement handle
    my $data = $Data{$self->{'*Id'}} ;  # data storage (Data is stored in a seperate hash to avoid circular references)

    if ($row <= $fetch && !$self->{'*EOD'} && defined ($sth))
        {

        # successfull select has happend before ?
        return undef if (!defined ($sth)) ;
        return undef if (defined ($max) && $row > $max) ;
        
        my $fld = $self->{'*SelectFields'} ;
        my $arr  ;
        my $i  ;

	if ($self -> {'*StoreAll'})
	    {
	    while ($row < $fetch)
		{
    	        if (!($arr = $sth -> fetchrow_arrayref ()))
		    {
		    $self->{'*EOD'} = 1 ;
		    $sth -> finish ;
		    last ;
		    }
                
                $i = 0 ;
                $data->[$row] = [ @$arr ] ;
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
		    $sth -> finish ;
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
	my $obj ;

        $dat = $data -> [$fetch] ;
	if (ref $dat eq 'ARRAY')
	    { # just an Array so tie it now
	    my $arr = $dat ;	
            $dat = {} ;
            $obj = tie %$dat, 'DBIx::Recordset::Row', $self, $self->{'*SelectFields'} , $arr ;
            $data -> [$fetch] = $dat ;
            $self->{'*LastKey'} = $obj -> FETCH ($self -> {'*PrimKey'}) ;
	    }
	else
	    {
	    #my $v ;
	    #my $k ;
	    #print LOG "old dat\n" ; #  = $dat  ref = " . ref ($dat) . " tied = " . ref (tied(%$dat)) . " fetch = $fetch\n"  ;
	    #while (($k, $v) = each (%$dat))
	    #        {
	    #        print "$k = $v\n" ;
	    #        }


	    my $obj = tied(%$dat) if ($dat) ;
	    $self->{'*LastRow'} = $fetch ;
	    $self->{'*LastKey'} = $obj?($obj -> FETCH ($self -> {'*PrimKey'})):undef ;
	    }
        }

    $self->{'*LastRecord'} = $dat ;
    $self->{'*LastRecordFetch'} = $fetch ;

    print LOG 'DB:  FETCH return ' . ($dat?$dat:'<undef>') . "\n"  if ($self->{'*Debug'} > 3) ;
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
## BuildFields ...
##

sub BuildFields

    {
    my ($self, $fields, $table, $tabrel) = @_ ;


    my @fields ;
    my $tab4f  = $self -> {'*Table4Field'} ;
    my $fnames = $self -> {'*FullNames'} ;
    my $debug  = $self -> {'*Debug'} ;
    my $drv    = $self->{'*Driver'} ;
    my %tables ;
    my %fields ;
    my %tabrel ;
    my @replace ;
    my $linkname ;
    my $link ;
    my $nf ;
    my $fn ;
    my @allfields ;
    my @orderedfields ;
    my $i ;
    my $n ;
    my $m ;
    my %namefields ;

    my $leftjoin = DBIx::Compat::GetItem ($drv, 'SupportSQLJoin') ;
    my $numtabs = 99 ;
    
    $numtabs = 2 if (DBIx::Compat::GetItem ($drv, 'SQLJoinOnly2Tabs')) ;


    %tables = map { $_ => 1 } split (/\s*,\s*/, $table) ;
    $numtabs -= keys %tables ;

    #print LOG "###--> numtabs = $numtabs\n" ;
    if (defined ($fields) && !($fields =~ /^\s*\*\s*$/))
        {
        @allfields = map { (/\./)?$_:"$tab4f->{$_}.$_" } split (/\s*,\s*/, $fields) ;
        #print LOG "###allfileds = @allfields\n" ;
	}
    else
        {
        @allfields = @$fnames ;
        }

    $nf = $self -> {'*NameField'} || $self -> TableAttr ('!NameField') ;
    if ($nf)
	{
	if (ref ($nf) eq 'ARRAY')
	    {
	    %namefields = map { ($fn = "$tab4f->{$_}\.$_") => 1 } @$nf ;
	    }
	else
	    {
	    %namefields = ( "$tab4f->{$nf}.$nf" => 1 ) ;
	    }

	@orderedfields = keys %namefields ;
	foreach $fn (@allfields)
	    {
	    push @orderedfields, $fn if (!$namefields{$fn}) ;
	    }
	}
    else
	{
	@orderedfields = @allfields ;
	}

    $i = 0 ;
    %fields = map { $_ => $i++ } @orderedfields ;

    $n = $#orderedfields ;
    $m = $n + 1;
    for ($i = 0; $i <=$n; $i++)
        {
        #print LOG "###loop numtabs = $numtabs\n" ;
	$fn = $orderedfields[$i] ;
        $replace[$i] = [$i] ;
        next if ($numtabs <= 0) ;
        next if (!($linkname = $self -> Link4Field ($fn))) ;
        next if (!($link = $self -> Link ($linkname))) ;
            # does not work with another Datasource or with an link to the table itself
        next if ($link -> {'!DataSource'} || $link -> {'!Table'} eq $self -> {'!Table'}) ; 
        
        $nf = $link->{'!NameField'} || $self -> TableAttr ('!NameField', undef, $link->{'!Table'}) ;

        if (!$link -> {'!LinkedBy'} && $nf)
            {
            $replace[$i] = [] ;
            if (ref $nf)
                {
                foreach (@$nf)
                    { 
                    if (!exists $fields{"$link->{'!Table'}.$_"})
                        {
                        push @orderedfields, "$link->{'!Table'}.$_" ;
                        push @allfields, "$link->{'!Table'}.$_" ;
                        $fields{"$link->{'!Table'}.$_"} = $m ; 
                        push @{$replace[$i]}, $m ;

                        print LOG "[$$] DB:  Add to $self->{'*Table'} linked name field $link->{'!Table'}.$_ (i=$i, n=$n, m=$m)\n" if ($debug > 2) ;            
                        $m++ ;
                        }
                    }
                }
            else
                {
                if (!exists $fields{"$link->{'!Table'}.$nf"})
                    {
                    push @orderedfields, "$link->{'!Table'}.$nf" ;
                    push @allfields, "$link->{'!Table'}.$nf" ;
                    $fields{"$link->{'!Table'}.$nf"} = $m ; 
                    push @{$replace[$i]}, $m ;

                    print LOG "[$$] DB:  Add to $self->{'*Table'} linked name field $link->{'!Table'}.$nf (i=$i, n=$n, m=$m)\n" if ($debug > 2) ;            
                    $m++ ;
                    }
                }

            $numtabs-- if (!exists $tables{$link->{'!Table'}}) ;
	    $tables{$link->{'!Table'}} = "$fn = $link->{'!Table'}.$link->{'!LinkedField'}" ;
            }
        elsif ($debug > 2 && !$link -> {'!LinkedBy'})
            { print LOG "[$$] DB:  No name, so do not add to $self->{'*Table'} linked name field $link->{'!Table'}.$fn\n" ;}            
        }

    #my $rfields = join (',', @allfields) ;
    my $rfields = join (',', @orderedfields) ;
    my $rtables = join (',', keys %tables) ;

    delete $tables{$table} ;
    my $rtabrel ;
    
    if ($leftjoin == 1)
	{
	$rtabrel = $table . ' ' . join (' ', map { "left join $_ on $tables{$_}" } keys %tables) ;
	}
    elsif ($leftjoin == 2)
	{
	my $v ;

	$tabrel = ($tabrel?"$tabrel and ":'') . join (' and ', map { $v = $tables{$_} ; $v =~ s/=/*=/ ; $v } keys %tables) ;
	}
    else 
	{
	my $v ;

	$tabrel = ($tabrel?"$tabrel and ":'') . join (' and ', map { "$tables{$_} (+)" } keys %tables) ;
	}


    return ($rfields, $rtables, $rtabrel, $tabrel, \@replace) ;
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
    my $linkname     = $self->{'*LinkName'} ;
    my $tab4f        = $self->{'*Table4Field'} ;
    my $ifunc        = $self->{'*InputFunctions'} ;
	
    
    if (!ref($where))
        { # We have the where as string
        $expr = $where ;
        if ($Debug > 2) { print LOG "DB:  Literal where -> $expr\n" ; }
        }
    elsif (defined ($primkey = $self->{'*PrimKey'}) && defined ($$where{$primkey}))
        {
        my $oper = $$where{"\*$primkey"} || '=' ;

        my $pkey = $primkey ;
        $pkey = "$tab4f->{$primkey}.$primkey" if ($linkname && !($primkey =~ /\./)) ;

        # any input conversion ?
	my $val = $$where{$primkey} ;
	my $if  = $ifunc -> {$primkey} ; 
	$val = &{$if} ($val) if ($if) ;

        if ($placeholders)
            { $expr = "$pkey$oper ? "; push @$bind_values, $val ; }
        elsif ($$Quote{$primkey})
            { $expr = "$pkey$oper" . $self->{'*DBHdl'} -> quote ($val) ; }
        else        
            { local $^W = 0 ; $expr = "$pkey$oper" . ($val+0) ; }
        if ($Debug > 2) { print LOG "DB:  Primary Key $primkey found -> $expr\n" ; }
        }
    else
        {         
        my $key ;
        my $lkey ;
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
	my $uright ;
        
	$econj = '' ;
    
 
        while (($key, $val) = each (%$where))
            {
            $type  = substr ($key, 0, 1) || ' ' ;
            $val = undef if ($ignore > 1 && $val eq '') ;

            if ($Debug > 2) { print LOG "DB:  SelectWhere <$key>=<$val> type = $type\n" ; }

            if (($type =~ /^(\w|\\|\+|\'|\#|\s)$/) && !($ignore && !defined ($val)))
                {
                if ($type eq '+')
                    { # composite field
                
                    if ($Debug > 3) { print LOG "DB:  Composite Field $key\n" ; }

                    $fconj    = '' ;
                    $fieldexp = '' ;
                    @fields   = split (/\&|\|/, substr ($key, 1)) ;

                    $multcnt = 0 ;
                    foreach $field (@fields)
                        {
                        if ($Debug > 3) { print LOG "DB:  Composite Field processing $field\n" ; }

                        if (!defined ($$Quote{lc($field)}))
                            {
                            if ($Debug > 2) { print LOG "DB:  Ignore non existing Composite Field $field\n" ; }
                            next ;
                            } # ignore no existent field

                        $op = $$where{"*$field"} || $oper ;

                        $field = "$tab4f->{$field}.$field" if ($linkname && !($field =~ /\./)) ;

                        if (($uright = $unaryoperators{lc($op)}))
			    {
    			    $multcnt-- ;
			    if ($uright == 1)
				{ $fieldexp = "$fieldexp $fconj $field $op" }
			    else
				{ $fieldexp = "$fieldexp $fconj $op $field" }
			    }
			elsif ($placeholders && $type ne '\\')
                            { $fieldexp = "$fieldexp $fconj $field $op \$val" ; $multcnt++ ; }
                        elsif (!defined ($val))
                            { $fieldexp = "$fieldexp $fconj $field $nullop NULL" ; }
                        elsif ($$Quote{lc($field)} && $type ne '\\')
                            { $fieldexp = "$fieldexp $fconj $field $op '\$val'" ; }
                        else
                            { $fieldexp = "$fieldexp $fconj $field $op \" . (\$val+0) . \"" ; }

                        $fconj ||= $$where{'$compconj'} || ' or ' ; 

                        if ($Debug > 3) { print LOG "DB:  Composite Field get $fieldexp\n" ; }

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

                    $lkey = lc ($key) ;

                    	    
		    if ($type eq "'")
                        {
                        $$Quote{$lkey} = 1 ;
                        }
                    elsif ($type eq '#')
                        {
                        $$Quote{$lkey} = 0 ;
                        }

                    # any input conversion ?
		    my $if  = $ifunc -> {$lkey} ; 
		    $val = &{$if} ($val) if ($if) ;
		    
		    {
		    local $^W = 0 ; # avoid warnings

		    #$val += 0 if ($$Quote{$lkey}) ; # convert value to a number if necessary
		    }

                    if (!defined ($$Quote{$lkey}) && $type ne '\\')
                        {
                        if ($Debug > 3) { print LOG "DB:  Ignore Single Field $key\n" ; }
                        next ; # ignore no existent field
                        } 

                    if ($Debug > 3) { print LOG "DB:  Single Field $key\n" ; }

                    $op = $$where{"*$key"} || $oper ;

                    $key = "$tab4f->{$lkey}.$key" if ($linkname && $type ne '\\' && !($key =~ /\./)) ;

                    if (($uright = $unaryoperators{lc($op)}))
			{
			$multcnt-- ;
			if ($uright == 1)
			    { $fieldexp = "$key $op" }
			else
			    { $fieldexp = "$op $key" }
			}
                    elsif (!$placeholders && defined ($val) && $$Quote{$lkey} && $type ne '\\')
                        { $fieldexp = "$key $op '\$val'" ; }
                    elsif (defined ($val) || $placeholders)
                        { $fieldexp = "$key $op \$val" ; }
                    else
                        { $fieldexp = "$key $nullop NULL" ; }

                    if ($Debug > 3) { print LOG "DB:  Single Field gives $fieldexp\n" ; }
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

                if ($Debug > 3) { print LOG "DB:  Key $key gives $vexp\n" ; }
            
            
                $expr = "$expr $econj ($vexp)" ;
            
                $econj ||= $$where{'$conj'} || ' and ' ; 
                }
            if ($Debug > 3 && $lexpr ne $expr) { $lexpr = $expr ; print LOG "DB:  expr is $expr\n" ; }
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
        elsif ($self->{'*Debug'} > 2)  { print LOG "DB:  CheckFields del $key = $val \n" ; }
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
    
    return if ($self -> {'*InFlush'}) ; # avoid endless recursion
    
    my $release = shift ;
    my $dat ;
    my $obj ;
    my $dbg = $self->{'*Debug'} ;
    my $id   = $self->{'*Id'} ;
    my $data = $Data{$id} ;
    my $rc = 1 ;

    print LOG "DB:  FLUSH Recordset id = $id  $self \n" if ($dbg > 2) ;

    $self -> {'*InFlush'} = 1 ;
    $self -> {'*UndefKey'} = undef ; # invalidate record for undef hashkey
    $self->{'*LastRecord'} = undef ; 
    $self->{'*LastRecordFetch'} = undef ; 

    eval
        {    

        foreach $dat (@$data)
	    {
            $obj = tied (%$dat) ;
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
        } ;


    $self -> {'*InFlush'} = 0 ;

    croak $@ if ($@) ;

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
    my $ifunc = $self->{'*InputFunctions'} ;

    if ($placeholders)
        {
        while (($key, $val) = each (%$data))
            {
            $val = $$val if (ref ($val) eq 'SCALAR') ;
            # any input conversion ?
	    my $if = $ifunc -> {$key} ;
	    $val = &{$if} ($val) if ($if) ;
	    next if (!defined ($val)) ; # skip NULL values
	    if ($key =~ /^\\(.*?)$/)
		{
                push @qvals, $val ;
                push @keys, $1 ;
                }
	    elsif (defined ($$Quote{lc($key)}))
                {
                push @bind_values ,$val ;
                push @qvals, '?' ;
                push @keys, $key ;
                }
            }
        }
    else
        {
        local $^W = 0 ;
        while (($key, $val) = each (%$data))
            {
            $val = $$val if (ref ($val) eq 'SCALAR') ;
            # any input conversion ?
	    my $if = $ifunc -> {$key} ;
	    $val = &{$if} ($val) if ($if) ;
	    next if (!defined ($val)) ; # skip NULL values
	    if ($key =~ /^\\(.*?)$/)
		{
                push @qvals, $val ;
                push @keys, $1 ;
                }
            elsif (($q = $$Quote{lc($key)}))
                {
                push @qvals, $self->{'*DBHdl'} -> quote ($val) ;
                push @keys, $key ;
                }
            elsif (defined ($q))
                {
                push @qvals, ($if?$val:($val+0)) ;
                push @keys, $key ;
                }
            }
        }

    my $rc = undef ;

    if ($#qvals > -1)
        {
        my $valstr = join (',', @qvals) ;
        my $keystr = join (',', @keys) ;

        $rc = $self->SQLInsert ($keystr, $valstr, \@bind_values) ;
        }
    

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
    my $ifunc = $self->{'*InputFunctions'} ;
    my $primkey ;

    if (defined ($primkey = $self->{'*PrimKey'}))
	{
        $val = $data -> {$primkey} ;
	$val = $$val if (ref ($val) eq 'SCALAR') ;
	#print LOG "1 primkey = $primkey d=$data->{$primkey} w=" . ($where?$where->{$primkey}:'<undef>') . " v=$val\n" ;
	if (defined ($val) && !$where)
	    {
	    $where = {$primkey => $val} ;
	    }
	elsif (ref ($where) eq 'HASH' && $val eq $where -> {$primkey})
	    {
	    delete $data -> {$primkey} ;
	    }
	else
	    {
	    $primkey = undef ;
	    }
	}
    else
	{
	$primkey = undef ;
	}

    #print LOG "2 primkey = $primkey d=$data->{$primkey} w=" . ($where?$where->{$primkey}:'<undef>') . " v=$val\n" ;
    my $datacnt = 0 ; 

    if ($placeholders)
        {
        while (($key, $val) = each (%$data))
            {
	    next if ($key eq $primkey) ;
	    $val = $$val if (ref ($val) eq 'SCALAR') ;
            # any input conversion ?
	    my $if = $ifunc -> {$key} ;
	    $val = &{$if} ($val) if ($if) ;
	    if ($key =~ /^\\(.*?)$/)
		{
                push @vals, "$1=$val" ;
                $datacnt++ ;
                }
            elsif (defined ($$Quote{lc($key)}))
                { 
                push @vals, "$key=?" ;
                push @bind_values, $val ;
                $datacnt++ ;
                }
            }
        }
    else
        {
        local $^W = 0 ;
	while (($key, $val) = each (%$data))
            {
	    next if ($key eq $primkey) ;
            $val = $$val if (ref ($val) eq 'SCALAR') ;
            # any input conversion ?
	    my $if = $ifunc -> {$key} ;
	    $val = &{$if} ($val) if ($if) ;

	    if ($key =~ /^\\(.*?)$/)
		{ 
		push @vals, "$1=$val" ;
                $datacnt++ ;
    		}
            else
		{
		if (defined ($q = $$Quote{lc($key)}))
		    {
		    if (!defined ($val))
			{ push @vals, "$key=NULL" ; }
		    elsif ($q)
			{ push @vals, "$key=" . $self->{'*DBHdl'} -> quote ($val) ; }
		    else
			{ push @vals, "$key=" . ($if?$val:($val+0)) ; }
                    $datacnt++ ;
		    }
		}

            }
        }

    my $rc = 0 ;
    if ($datacnt)
	{
	my $valstr = join (',', @vals) ;

	if (defined ($where))
	    { $expr = $self->BuildWhere ($where, \@bind_values) ; }
	else
	    { $expr = $self->BuildWhere ($data, \@bind_values) ; }


	$rc = $self->SQLUpdate ($valstr, $expr, \@bind_values) ;
	}

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
    print LOG "DB:  UpdateInsert update returns: $rc  affected rows: $DBI::rows\n" if ($self->{'*Debug'} > 2) ;
    
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

    $self->{'*LastKey'} = undef ;

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

    my $rc = $self->SQLSelect ($expr, $self->{'*Fields'} || $fields, $self->{'*Order'} || $order, $group, $append, \@bind_values) ;
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

    $start = 0 if (defined ($$fdat{'$first'}) || (defined ($start) && $start < 0)) ;
    $max   = 1 if (defined ($max) && $max < 1) ;

    if (defined ($$fdat{'$prev'}))
        {
        $start -= $max ; 
        if ($start < 0) { $start = 0 ; }
        }
    elsif (defined ($$fdat{'$next'}))
        { $start += $max ; }
    elsif (defined ($$fdat{'$goto'}))
        { 
	$start = $$fdat{'$gotorow'} - 1 ;
        if ($start < 0) { $start = 0 ; }
	}


    my $rc = $self->Select($fdat, $$fdat{'$fields'}, $$fdat{'$order'}, $$fdat{'$group'}, $$fdat{'$append'}) ; 
    
    if ($rc && $$fdat{'$last'})
	{ # read all until last row
	my $storeall = $self->{'*StoreAll'} ;
	$self->{'*StoreAll'} = 1 ;
	$self -> FETCH (0x7ffffff) ;
	$start = $self->{'*LastRow'} - ($max || 1) ;
	$self->{'*StoreAll'} = $storeall ;
	}

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


    if ($self->{'*Debug'} > 2)
         { print LOG 'DB:  Execute ' . ($$fdat{'=search'}?'=search ':'') .
                ($$fdat{'=update'}?'=update ':'') . ($$fdat{'=insert'}?'=insert ':'') .
                ($$fdat{'=empty'}?'=empty':'') . ($$fdat{'=delete'}?'=delete':'') . "\n" ; }

    my $rc = '-' ;
    if (defined ($$fdat{'=search'})) 
        {
        $rc = $self -> Search ($fdat) 
        }
    else
        {
        #$rc = $self -> UpdateInsert ($fdat) if (defined ($$fdat{'=update'}) && defined ($$fdat{'=insert'}) && !defined($rc)) ;
        $rc = $self -> Update ($fdat) if (defined ($$fdat{'=update'}) && $rc eq  '-') ;
        $rc = $self -> Insert ($fdat) if (defined ($$fdat{'=insert'}) && $rc eq  '-') ;
        $rc = $self -> Delete ($fdat) if (defined ($$fdat{'=delete'}) && $rc eq  '-') ;
        $rc = $self -> Search ($fdat) if (!defined ($$fdat{'=empty'}) && defined ($rc)) ;
        $rc = 1 if (defined ($$fdat{'=empty'}) && $rc eq  '-') ;
        }
                
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

  
    my $param = $textprev ;
    my $textfirst ;
    my $textlast ;
    my $textgoto ;
    
    if (ref $textprev eq 'HASH')
	{
	$fdat = $textnext ;
	$textprev  = $param -> {-prev} ;  
	$textnext  = $param -> {-next} ;  
	$textfirst = $param -> {-first} ;  
	$textlast  = $param -> {-last} ;
	$textgoto  = $param -> {-goto} ;
	}
	  
  
  
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
            if ($k ne '$start' && $k ne '$max' && $k ne '$prev' && $k ne '$next' && $k ne '$goto' && $k ne '$gotorow'
	         && $k ne '$first' && $k ne '$last')
        	    {
	            $buttons .= "$esc<input type=$esc\"hidden$esc\" name=$esc\"" . $k . "$esc\" value=$esc\"$v$esc\"$esc>\n" ;
		    }
            }
        }

    if ($start > 0 && $textfirst)
        {
        $buttons .= "$esc<input type=$esc\"submit$esc\" name=$esc\"\$first$esc\" value=$esc\"$textfirst$esc\"$esc> " ;
        }
    if ($start > 0 && $textprev)
        {
        $buttons .= "$esc<input type=$esc\"submit$esc\" name=$esc\"\$prev$esc\" value=$esc\"$textprev$esc\"$esc> " ;
        }
    if ($textgoto)
        {
        $buttons .= "$esc<input type=$esc\"text$esc\" size=6 name=$esc\"\$gotorow$esc\"$esc>" ;
        $buttons .= "$esc<input type=$esc\"submit$esc\" name=$esc\"\$goto$esc\" value=$esc\"$textgoto$esc\"$esc> " ;
        }
    if ($more > 0 && $textnext)
        {
        $buttons .= "$esc<input type=$esc\"submit$esc\" name=$esc\"\$next$esc\" value=$esc\"$textnext$esc\"$esc> " ;
        }
    if ($more > 0 && $textlast)
        {
        $buttons .= "$esc<input type=$esc\"submit$esc\" name=$esc\"\$last$esc\" value=$esc\"$textlast$esc\"$esc>" ;
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
    if (ref $_[2] eq 'ARRAY')
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
    
	{
	local $^W = 0 ;
        print DBIx::Recordset::LOG "DB:  ::CurrRow::DESTROY\n" if ($self -> {'*Recordset'} -> {'*Debug'} > 3) ;
	}
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

    return $rs-> {'*UndefKey'} if (!defined ($fetch)) ;  # undef could be used as key for autoincrement values
    
    my $h ;

    print DBIx::Recordset::LOG "DB:  Hash::FETCH \{" . (defined ($fetch)?$fetch:'<undef>') ."\}\n"  if ($rs->{'*Debug'} > 3) ;

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

    print DBIx::Recordset::LOG "DB:  Hash::FETCH return " . defined ($h)?$h:'<undef>' . "\n" if ($rs->{'*Debug'} > 3) ;
    
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

    print DBIx::Recordset::LOG "DB:  ::Hash::STORE \{$key\} = $value\n" if ($rs->{'*Debug'} > 3) ;

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
    $rs-> {'*UndefKey'} = defined($key)?undef:$value ;
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
    
    if ($rs->{'*Debug'} > 3) 
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

    if ($rs->{'*Debug'} > 3) 
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
    my ($self, $key) = @_ ;
    my $rs    = $self -> {'*Recordset'} ;  
    
    $rs->{'*LastKey'} = undef ;
    
    if ($rs->{'*Placeholders'})
        { $rs->SQLDelete ("$rs->{'*PrimKey'} = ?", [$key]) or return undef ; }
    elsif ($rs->{'*Quote'}{$rs->{'*PrimKey'}})
        { $rs->SQLDelete ("$rs->{'*PrimKey'} = " . $rs->{'*DBHdl'} -> quote ($key)) or return undef ; }
    else        
        { $rs->SQLDelete ("$rs->{'*PrimKey'} = " . ($key+0)) or return undef ; }

    return 1 ;
    }
                
## ----------------------------------------------------------------------------

sub CLEAR 

    {
    my ($self, $key) = @_ ;
    my $rs    = $self -> {'*Recordset'} ;  

    $rs->SQLDelete ('') or return undef ; 
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

	{
	local $^W = 0 ;
        print DBIx::Recordset::LOG "DB:  ::Hash::DESTROY\n" if ($self -> {'*Recordset'} -> {'*Debug'} > 3) ;
	}
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
            my $dirty = 0 ;
            $self->{'*new'}     = 1 ;                  # mark it as new record
            
            
            my $lk ;
            while (($k, $v) = each (%$names))
                {
                $lk = lc ($k) ;
                # store the value and remeber it for later update
                $upd ->{$lk} = \($data->{$lk} = $v) ;
                $dirty = 1 ;
                }
            $self->{'*dirty'}   = $dirty ;             # mark it as dirty only if data exists
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
	my $of ;
        my $ofunc    = $rs -> {'*OutputFuncArray'} || [] ;
	my $linkname = $rs -> {'*LinkName'} ;
	if ($linkname < 2)
            {    
            $i = -1 ;
	    %$data = map { $i++ ; lc($$names[$i]) => ($ofunc->[$i]?(&{$ofunc->[$i]}($_)):$_) } @$dat if ($dat) ;
            }
        elsif ($linkname < 3)
            {
            my $r ;
            my $repl = $rs -> {'*ReplaceFields'} ;
            my $n ;
                
            foreach $r (@$repl)
                {
                $n = lc ($names -> [$i]) ;
                $of = $ofunc -> [$i] ;
		$data -> {$n} = ($of?(&{$of}($dat->[$i])):$dat->[$i]) ;
                $data -> {uc($n)} = join (' ', map ({ ($ofunc->[$_]?(&{$ofunc->[$_]}($dat->[$_])):$dat->[$_])} @$r)) if ($#$r > 0 || $r -> [0] != $i) ;
                $i++ ;
                }
            }
        else
            {
            my $r ;
            my $repl = $rs -> {'*ReplaceFields'} ;
                
            foreach $r (@$repl)
                {
                $data -> {lc ($$names[$i])} = join (' ', map ({ ($ofunc->[$_]?(&{$ofunc->[$_]}($dat->[$_])):$dat->[$_])} @$r)) ;
		#print LOG "###repl $r -> $data->{$$names[$i]}\n" ;
                $i++ ;
                }
            }
        
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
    my ($self, $key, $value)  = @_ ;
    my $rs  = $self -> {'*Recordset'} ;  
    my $dat = $self -> {'*data'} ;
    print DBIx::Recordset::LOG "DB:  Row::STORE $key = $value\n" if ($rs->{'*Debug'} > 3) ;
    # any changes?
    if ($dat -> {$key} ne $value || defined ($dat -> {$key}) != defined($value))
	{
	# store the value and remeber it for later update
	$self -> {'*upd'}{$key} = \($dat -> {$_[1]} = $value) ;
	$self -> {'*dirty'}   = 1 ;                  # mark row dirty
	}
    }

## ----------------------------------------------------------------------------

sub FETCH
    {
    my ($self, $key) = @_ ;
    return undef if (!$key) ;
    my $rs   = $self -> {'*Recordset'} ;  
    my $data = $self -> {'*data'}{$key} ;
    my $link ;
    if (!defined($data))
        {
        if ($key eq '!Name')
            {
            my $nf = $rs -> {'*NameField'} || $rs -> TableAttr ('!NameField') ;
            if (!ref $nf)
                {
                return $self -> {'*data'}{$key} = $self -> {'*data'}{uc($nf)} || $self -> {'*data'}{$nf} ;
                }
            
            return $self -> {'*data'}{$key} = join (' ', map { $self -> {'*data'}{uc ($_)} || $self -> {'*data'}{$_} } @$nf) ;
            }
        elsif (defined ($link = $rs -> {'*Links'}{$key}))
            {
            my $lf = $link -> {'!LinkedField'} ;
            my $dat = $self -> {'*data'} ;
	    my $mv ;
	    if (exists ($dat -> {$link -> {'!MainField'}}))
		{ 
		$mv = $dat -> {$link -> {'!MainField'}} ;
		}
	    else
		{ 
		$mv = $dat -> {"$link->{'!MainTable'}.$link->{'!MainField'}"} ;
		}
	    my $setup = {%$link} ;
            $setup -> {$lf} = $mv ;
            $setup -> {'!Default'} = { $lf => $mv } ;
            $setup -> {'!DataSource'} = $rs if (!defined ($link -> {'!DataSource'})) ;
            $data = $self -> {'*data'}{$key} = DBIx::Recordset -> Search ($setup) ;
            #delete $link -> {'!Recordset'} if (ref ($link -> {'!DataSource'})) ; # avoid backlinks
            print DBIx::Recordset::LOG "DB:  Row::FETCH $key = Setup New Recordset for table $link->{'!Table'}, $lf= <$mv>\n" if ($rs->{'*Debug'} > 3) ;
	    my $of = $rs -> {'*OutputFunctions'}{$key} ;
	    return &{$of}($data) if ($of) ;	    
            return $data ;
            }
        }

    print DBIx::Recordset::LOG "DB:  Row::FETCH $key = <" . $data . ">\n" if ($rs->{'*Debug'} > 3) ;
    
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
    
    return 1 if (!$rs) ;

    if ($self -> {'*dirty'}) 
        {
        my $rc ;
	print DBIx::Recordset::LOG "DB:  Row::Flush id=$rs->{'*Id'} $self\n" if ($rs->{'*Debug'} > 3) ;

        my $dat = $self -> {'*upd'} ;
        if ($self -> {'*new'})
            {
            $rc = $rs -> Insert ($dat)  ;
	    }
        else
            {
            my $pko ;
            my $pk = $rs -> {'*PrimKey'} ;
            $dat->{$pk} = \($self -> {'*data'}{$pk}) if ($pk && !exists ($dat->{$pk})) ;
            #carp ("Need primary key to update record") if (!exists($self -> {"=$pk"})) ;
            if (!exists($self -> {'*PrimKeyOrgValue'})) 
                {
                $rc = $rs -> Update ($dat)  ;
                }
            elsif (ref ($pko = $self -> {'*PrimKeyOrgValue'}) eq 'HASH')
                {
                $rc = $rs -> Update ($dat, $pko)  ;
                }
            else
                {
                $rc = $rs -> Update ($dat, {$pk => $pko} )  ;
                }
            }
        
	return undef if (!defined($rc)) ;

	delete $self -> {'*new'} ;
        delete $self -> {'*dirty'} ;
        $self -> {'*upd'} = {} ;
        }

    my $k ;
    my $v ;
    my $lrs ;
    # "each" is not reentrant !!!!!!!!!!!!!!
    #while (($k, $v) = each (%{$rs -> {'*Links'}}))
    foreach $k (keys %{$rs -> {'*Links'}})
        { # Flush linked tables
        
        $lrs = $self->{'*data'}{$k} ;
        ${$lrs} -> Flush () if (defined ($lrs)) ;
        }

    return 1 ;
    }



## ----------------------------------------------------------------------------

sub DESTROY

    {
    my $self = shift ;
    
	{
	local $^W = 0 ;
        print DBIx::Recordset::LOG "DB:  Row::DESTROY\n" if ($DBIx::Recordset::Debug > 2 || $self -> {'*Recordset'} -> {'*Debug'} > 3) ;
	}

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

B<DBIx::Recordset> is a Perl module, which should make it easier to access a set
of records in a database.
It should make standard database access (select/insert/update/delete)
easier to handle (e.g. web applications or scripts to enter/retrieve
data to/from a database). Special attention is made for web applications to make
it possible to handle state-less access and to process the posted data
of formfields.
The programmer only has to supply the absolutely necessary information, the
rest is done by DBIx::Recordset.

B<DBIx::Recordset> uses the DBI API to access the database, so it should work with
every database for which a DBD driver is available (see also DBIx::Compat)

Most public functions take a hash reference as parameter, which makes it simple
to supply various different arguments to the same function. The parameter hash
can also be taken from a hash containing posted formfields like those available with
CGI.pm, mod_perl, HTML::Embperl and others.

Before using a recordset it is necessary to setup an object. Of course the
setup step can be made with the same function call as the first database access,
but it can also be handled separately.

Most functions which set up an object return a B<typglob>. A typglob in Perl is an 
object which holds pointers to all datatypes with the same name. Therefore a typglob
must always have a name and B<can't> be declared with B<my>. You can only
use it as B<global> variable or declare it with B<local>. The trick for using
a typglob is that setup functions can return a B<reference to an object>, an
B<array> and a B<hash> at the same time.

The object is used to access the object's methods, the array is used to access
the records currently selected in the recordset and the hash is used to access
the current record.

If you don't like the idea of using typglobs you can also set up the object,
array and hash separately, or just set the ones you need.

=head1 ARGUMENTS

Since most methods take a hash reference as argument, here is a
description of the valid arguments first.

=head2 Setup Parameters

All parameters starting with an '!' are only recognized at setup time.
If you specify them in later function calls they will be ignored.

=item B<!DataSource>

Specifies the database to which to connect. This information can be given in
the following ways:

=over 4

=item Driver/DB/Host.

Same as the first parameter to the DBI connect function.

=item DBIx::Recordset object

Takes the same database handle as the given DBIx::Recordset object.

=item DBIx::Database object

Takes Driver/DB/Host from the given database object.

=item DBIx::Datasbase object name

Takes Driver/DB/Host from the database object which is saved under
the given name ($saveas parameter to DBIx::Database -> new)

=back

=item B<!Table>

Tablename, multiple tables are comma-separated.

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

NOTE 2: The keys for the returned hash normaly doesn't have a table part, only the fieldname
part forms the key. (see !LongNames for an execption)

NOTE 3: Because the query result is returned in a hash, there can only be
one out of multiple fields with the same name fetched at once.
If you specify multiple fields with the same name, only one is returned
from a query. Which one this actually is depends on the DBD driver.
(see !LongNames for an execption)

NOTE 4: Some databases (e.g. mSQL) require you to always qualify a fieldname
with a tablename if more than one table is accessed in one query.

=item B<!LongNames>

When set to 1 the keys of the hash which is returned for each record not only
consits of the fieldname, but are build in the form table.field. In the current
version this only works if you retrieve all fields (i.e. !Field is missing or
contains '*')

=item B<!Order>

Fields which should be used for ordering any query. If you have specified multiple
tables the fieldnames should be unique. If the names are not unique you must
specify them among with the tablename (e.g. tab1.field).


NOTE 1: Fieldnames specified with !Order can't be overridden. If you plan
to use other fields with this object later, use $order instead.


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

  SELECT * FROM tab1 LEFT JOIN tab2 ON	(tab1.id=tab2.id) WHERE name = 
'foo' ;



=item B<!PrimKey>

Name of primary key. If specified, DBIx::Recordset assumes that this is a unique
key to the given table(s). DBIx::Recordset can not verify this; you are responsible
for specifying the right key. If such a primary key exists in your table, you
should specify it here, because it helps DBIx::Recordset to optimize the building
of WHERE expressions.

=item B<!WriteMode>

!WriteMode specifies which write operations to the database are allowed and which are
disabled. You may want to set !WriteMode to zero if you only need to query data, to
avoid accidently changeing the content of the database.

B<NOTE:> The !WriteMode only works for the DBIx::Recordset methods. If you
disable !WriteMode, it is still possible to use B<do> to send normal
SQL statements to the database engine to write/delete any data.

!WriteMode consists of some flags, which may be added together:

=over 4

=item DBIx::Recordset::wmREADONLY (0)

Allow B<no> write access to the table(s)

=item DBIx::Recordset::wmINSERT (1)

Allow INSERT

=item DBIx::Recordset::wmUPDATE (2)

Allow UPDATE

=item DBIx::Recordset::wmDELETE (4)

Allow DELETE

=item DBIx::Recordset::wmCLEAR (8)

To allow DELETE for the whole table, wmDELETE must be also specified. This is 
necessary for assigning a hash to a hash which is tied to a table. (Perl will 
first erase the whole table, then insert the new data)

=item DBIx::Recordset::wmALL (15)

Allow every access to the table(s)


=back

Default is wmINSERT + wmUPDATE + wmDELETE


=item B<!StoreAll>

If present, this will cause DBIx::Recordset to store all rows which will be fetched between
consecutive accesses, so it's possible to access data in a random order. (e.g.
row 5, 2, 7, 1 etc.) If not specified, rows will only be fetched into memory
if requested, which means that you will have to access rows in ascending order.
(e.g. 1,2,3 if you try 3,2,4 you will get an undef for row 2 while 3 and 4 is ok)
see also B<DATA ACCESS> below.

=item B<!HashAsRowKey>

By default, the hash which is returned by the setup function is tied to the
current record. You can use it to access the fields of the current
record. If you set this parameter to true, the hash will by tied to the whole
database. This means that the key of the hash will be used as the primary key in
the table to select one row. (This parameter only has an effect on functions
which return a typglob).

=item B<!IgnoreEmpty>

This parameter defines how B<empty> and B<undefined> values are handled. 
The values 1 and 2 may be helpful when using DBIx::Recordset inside a CGI
script, because browsers send empty formfields as empty strings.

=over 4

=item B<0 (default)>

An undefined value is treated as SQL B<NULL>: an empty strings remains an empty 
string.

=item B<1>

All fields with an undefined value are ignored when building the WHERE expression.

=item B<2>

All fields with an undefined value or an empty string are ignored when building the 
WHERE expression.

=back

B<NOTE:> The default for versions before 0.18 was 2.

=item B<!Filter>

Filters can be used to pre/post-process the data which is read from/written to the database.
The !Filter parameter takes an hash reference which contains the filter fucntions. If the key
is numeric, it is treaded as a type value and the filter is applied to all fields of that 
type. If the key if alphanumeric, the filter applies to the named field. Every filter 
description consistst of an array which two element, the first element must contain the input
function and the second element must contain the output function. Either may be undef, if only
one of them are neccessary. The data is passed to the input function before it is written to the
database. The input function must return the value in the correct format for the database. The output
function get the data passed which is read from the database before it is return to the user.
 
 
 Example:

     '!Filter'   => 
	{
	DBI::SQL_DATE     => 
	    [ 
		sub { shift =~ /(\d\d)\.(\d\d)\.(\d\d)/ ; "19$3$2$1"},
		sub { shift =~ /\d\d(\d\d)(\d\d)(\d\d)/ ; "$3.$2.$1"}
	    ],

	'datefield' =>
	    [ 
		sub { shift =~ /(\d\d)\.(\d\d)\.(\d\d)/ ; "19$3$2$1"},
		sub { shift =~ /\d\d(\d\d)(\d\d)(\d\d)/ ; "$3.$2.$1"}
	    ],

	}

Both filters converts a date in the format dd.mm.yy to the database format 19yymmdd and
vice versa. The first one does this for all fields of the type SQL_DATE, the second one
does this for the fields with the name datefield.

The B<!Filter> parameter can also be passed to the function B<TableAttr> of the B<DBIx::Database>
object. In this case it applies to all DBIx::Recordset objects, which use this tables.



=item B<!LinkName>

This allows you to get a clear text description of a linked table, instead of (or in addition
to) the !LinkField. For example, if you have a record with all your bills, and each record contains
a customer number, setting !LinkName DBIx::Recordset can automatically retrieve the 
name of
the customer instead of (or in addition to) the bill record itself.

=over 4

=item 1 select additional fields

This will additionally select all fields given in B<!NameField> of the Link or the table
attributes (see TableAttr).

=item 2 build name in uppercase of !MainField

This takes the values of B<!NameField> of the Link or the table attributes (see 
TableAttr)
and joins the content of these fields together into a new field, which has the same name
as the !MainField, but in uppercase.


=item 2 replace !MainField with the contents of !NameField

Same as 2, but the !MainField is replaced with "name" of the linked record.

=back

See also B<!Links> and B<WORKING WITH MULTIPLE TABLES> below



=item B<!Links>

This parameter can be used to link multiple tables together. It takes a
reference to a hash, which has - as keys, names for a special B<"linkfield">
and - as value, a parameter hash. The parameter hash can contain all the
B<Setup parameters>. The setup parameters are taken to construct a new
recordset object to access the linked table. If !DataSource is omitted (as it
normally should be), the same DataSource (and database handle), as the
main object is taken. There are special parameters which can only 
occur in a link definition (see next paragraph). For a detailed description of
how links are handled, see B<WORKING WITH MULTIPLE TABLES> below.

=head2 Link Parameters

=item B<!MainField>

The B<!MailField> parameter holds a fieldname which is used to retrieve
a key value for the search in the linked table from the main table.
If omitted, it is set to the same value as B<!LinkedField>.

=item B<!LinkedField>

The fieldname which holds the key value in the linked table.
If omitted, it is set to the same value as B<!MainField>.

=item B<!NameField>

This specifies the field or fields which will be used as a "name" for the destination table. 
It may be a string or a reference to an array of strings.
For example, if you link to an address table, you may specfiy the field "nickname" as the 
name field
for that table, or you may use ['name', 'street', 'city'].

Look at B<!LinkName> for more information.





=head2 Where Parameters

The following parameters are used to build an SQL WHERE expression

=item B<{fieldname}>

Value for field. The value will be quoted automatically, if necessary.

=item B<'{fieldname}>

Value for field. The value will always be quoted. This is only necessary if
DBIx::Recordset cannot determine the correct type for a field.

=item B<#{fieldname}>

Value for field. The value will never be quoted, but will converted a to number.
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
with B<$valueconj>. By default, only one of the values must match the field.


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

First row to fetch. The row specified here will appear as index 0 in
the data array

=item B<$max>

Maximum number of rows to fetch. Every attempt to fetch more rows than specified
here will return undef, even if the select returns more rows.

=item B<$next>

Add the number supplied with B<$max> to B<$start>. This is intended to implement
a next button.

=item B<$prev>

Subtract the number supplied with B<$max> from B<$start>. This is intended to 
implement
a previous button.

=item B<$order>

Fieldname(s) for ordering (ORDER BY) (must be comma-separated, could also contain 
USING)

=item B<$group>

Fieldname(s) for grouping (GROUP BY) (must be comma-separated, could also contain 
HAVING)

=item B<$append>

String which is appended to the end of a SELECT statement, can contain any data.

=item B<$fields>

Fields which should be returned by a query. If you have specified multiple
tables the fieldnames should be unique. If the names are not unique you must
specify them along with the tablename (e.g. tab1.field).


NOTE 1: If B<!fields> is supplied at setup time, this can not be overridden
by $fields.

NOTE 2: The keys for the returned hash normaly doesn't have a table part, only the fieldname
part forms the key. (see !LongNames for an execption)

NOTE 3: Because the query result is returned in a hash, there can only be
one out of multiple fields  with the same name fetched at once.
If you specify multiple fields with same name, only one is returned
from a query. Which one this actually is, depends on the DBD driver.
(see !LongNames for an execption)

=item B<$primkey>

Name of primary key. DBIx::Recordset assumes that if specified, this is a unique
key to the given table(s). DBIx::Recordset can not verify this. You are responsible
for specifying the right key. If such a primary exists in your table, you
should specify it here, because it helps DBIx::Recordset optimize the building
of WHERE expressions.

See also B<!primkey>


=head2 Execute parameters

The following parameters specify which action is to be executed

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

Setup a new object and connect it to a database and table(s). Collects
information about the tables which are needed later. Returns a typglob
which can be used to access the object ($set), an array (@set) and a 
hash (%set).

B<params:> setup

=item B<$set = DBIx::Recordset -E<gt> SetupObject (\%params)>

Same as above, but setup only the object, do not tie anything (no array, no hash)

B<params:> setup

=item B<$set = tie @set, 'DBIx::Recordset', $set>

=item B<$set = tie @set, 'DBIx::Recordset', \%params>

Ties an array to a recordset object. The result of a query which is executed
by the returned object can be accessed via the tied array. If the array contents
are modified, the database is updated accordingly (see Data access below for
more details). The first form ties the array to an already existing object, the 
second one setup a new object.

B<params:> setup


=item B<$set = tie %set, 'DBIx::Recordset::Hash', $set>

=item B<$set = tie %set, 'DBIx::Recordset::Hash', \%params>

Ties a hash to a recordset object. The hash can be used to access/update/insert
single rows of a table: the hash key is identical to the primary key
value of the table. (see Data access below for more details)

The first form ties the hash to an already existing object, the second one
sets up a new object.

B<params:> setup



=item B<$set = tie %set, 'DBIx::Recordset::CurrRow', $set>

=item B<$set = tie %set, 'DBIx::Recordset::CurrRow', \%params>

Ties a hash to a recordset object. The hash can be used to access the fields
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

The second and third syntax selects from an existing DBIx::Recordset object.


B<params:> setup (only syntax 1), where  (without $order and $fields)

B<where:>  (only syntax 3) string for SQL WHERE expression

B<fields:> comma separated list of fieldnames to select

B<order:>  comma separated list of fieldnames to sort on



=item B<*set = DBIx::Recordset -E<gt> Search (\%params)>

=item B<set -E<gt> Search (\%params)>

Does a search on the given tables and prepares data to access them via
@set or %set. The first syntax also sets up a new object.

B<params:> setup (only syntax 1), where, search


=item B<*set = DBIx::Recordset -E<gt> Insert (\%params)>

=item B<$set -E<gt> Insert (\%params)>

Inserts a new record in the recordset table(s). Params should contain one
entry for every field for which you want to insert a value.

Fieldnames may be prefixed with a '\' in which case they are not processed (quoted)
in any way.

B<params:> setup (only syntax 1), fields



=item B<*set = DBIx::Recordset -E<gt> Update (\%params, $where)>

=item B<*set = DBIx::Recordset -E<gt> Update (\%params, $where)>

=item B<set -E<gt> Update (\%params, $where)>

=item B<set -E<gt> Update (\%params, $where)>

Updates one or more records in the recordset table(s). Parameters should contain
one entry for every field you want to update. The $where contains the SQL WHERE
condition as a string or as a reference to a hash. If $where is omitted, the
where conditions are buily from the parameters.

Fieldnames maybe prefixed with a '\' in which case they are not processed (quoted)
in any way.


B<params:> setup (only syntax 1+2), where (only if $where is omitted), fields



=item B<*set = DBIx::Recordset -E<gt> Delete (\%params)>

=item B<$set -E<gt> Delete (\%params)>

Deletes one or more records form the recordsets table(s)

B<params:> setup (only syntax 1), where


=item B<*set = DBIx::Recordset -E<gt> Execute (\%params)>

=item B<$set -E<gt> Execute (\%params)>

Executes one of the above methods, depending on the given arguments.
If multiple execute parameters are specified, the priority is
 =search
 =update
 =insert
 =delete
 =empty

If none of the above parameters are specified, a search is performed.


B<params:> setup (only syntax 1), execute, where, search, fields


=item B<$set -E<gt> do ($statement, $attribs, \%params)>

Same as DBI. Executes a single SQL statement on the open database.


=item B<$set -E<gt> First ()>

Position the record pointer to the first row.


=item B<$set -E<gt> Next ()>

Position the record pointer to the next row.


=item B<$set -E<gt> Prev ()>

Position the record pointer to the previous row.

=item B<$set -E<gt> AllNames ()>

Returns a reference to an array of all fieldnames of all tables
used by the object.

=item B<$set -E<gt> Names ()>

Returns a reference to an array of the fieldnames from the last
query.

=item B<$set -E<gt> AllTypes ()>

Returns a reference to an array of all fieldtypes of all tables
used by the object.

=item B<$set -E<gt> Types ()>

Returns a reference to an array of the fieldtypes from the last
query.

=item B<$set -E<gt> Add ()>

=item B<$set -E<gt> Add (\%data)>

Adds a new row to a recordset. The first one adds an empty row, the
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
recordset. If the $ignoremax parameter is specified and is true, 
MoreRecords ignores the $max parameter of the last Search.

To tell you if there are more records, More actually fetches the next
record from the database and stores it in memory. It does not, however, 
change the current record.

=item B<$set -E<gt> PrevNextForm ($prevtext, $nexttext, \%fdat)>

Returns a HTML form which contains a previous and a next button and
all data from %fdat, as hidden fields. When calling the Search method,
You must set the $max parameter to the number of rows
you want to see at once. After the search and the retrieval of the
rows, you can call PrevNextForm to generate the needed buttons for
scrolling through the recordset.


=item B<$set -E<gt> Flush>

The Flush method flushes all data to the database and therefore makes sure
that the db is up-to-date. Normally, DBIx::Recordset holds the update in memory
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



=item B<$set -E<gt> Begin>

Starts an transaction. Calls the DBI method begin.


=item B<$set -E<gt> Rollback>

Rolls back an transaction. Calls the DBI method rollback and makes sure that all 
internal buffers of DBIx::Recordset are flushed.


=item B<$set -E<gt> Commit>

Commits an transaction. Calls the DBI method commit and makes sure that all 
internal buffers of DBIx::Recordset are flushed.


=item B<$set -E<gt> DBHdl ()>

Returns the DBI database handle.


=item B<$set -E<gt> StHdl ()>

Returns the DBI statement handle of the last select.


=item B<$set -E<gt> StartRecordNo ()>

Returns the record number of the record which will be returned for index 0.


B<$set -E<gt> LastSQLStatement ()>

Returns the last executed SQL Statement.


B<$set -E<gt> Disconnect ()>

Closes the connection to the database.


B<$set -E<gt> Link($linkname)>

If $linkname is undef, returns reference to a hash of all links
of the object. Otherwise, it returns a reference to the link with 
the given name.


B<$set -E<gt> Link4Field($fieldname)>

Returns the name of the link for that field, or <undef> if
there is no link for that field.



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

2.) DBIx::Recordset holds a B<current record> which can be accessed directly via
a hash. The current record is the one you last accessed via the array. After
a Select or Search, it is reset to the first record. You can change the current
record via the methods B<Next>, B<Prev>, B<First>, B<Add>.

Example:
 $set{id}	    access the field 'id' of the current record
 $set{name}	    access the field 'name' of the current record



Instead of doing a B<Select> or B<Search> you can directly access one row
of a table when you have tied a hash to DBIx::Recordset::Hash or have
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
  # setup a new object and search all records with name xyz
  *set = DBIx::Recordset -> Search ({'!DataSource' => 'dbi:db:tab',
				     '!PrimKey => 'id',
				     '!Table'  => 'tabname',
				     'name'    => 'xyz'}) ;

  #now you can update an existing record by assigning new values
  #Note: if possible, specify a PrimKey for update to work faster
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
  # record
  $set -> Add () ;
  $set{'name'} = 'more foo' ;
  $set{'id'}   = 11 ;

  # or insert the data directly via Add
  $set -> Add ({'name' => 'even more foo',
		'id'   => 12}) ;

  # NOTE: up to this point, NO data is actually written to the db!

  # we are done with that object,  Undef will flush all data to the db
  DBIx::Recordset::Undef ('set') ;

IMPORTANT: The data is not written to the database until you explicitly
call B<flush>, or a new query is started, or the object is destroyed. This is 
to keep the actual writes to the database to a minimum.

=head1 WORKING WITH MULTIPLE TABLES

DBIx::Recordset has some nice features to make working with multiple tables
and their relations easier. 

=head2 Joins

First, you can specify more than one
table to the B<!Table> parameter. If you do so, you need to specifiy how both
tables are related. You do this with B<!TabRelation> parameter. This method
will access all the specified tables simultanously.

=head2 Join Example:

If you have the following two tables, where the field street_id is a 
pointer to the table street:

  table name
  name	    char (30),
  street_id  integer

  table street
  id	    integer,
  street    char (30),
  city      char (30)

You can perform the following search:

  *set = DBIx::Recordset -> Search ({'!DataSource' => 'dbi:drv:db',
		     '!Table'	   => 'name, street',
		     '!TabRelation'=> 'name.street_id = street.id'}) ;

The result is that you get a set which contains the fields B<name>, B<street_id>,
B<street>, B<city> and B<id>, where id is always equal to street_id. If there are multiple
streets for one name, you will get as many records for that name as there are streets
present for it. For this reason, this approach works best when you have a 
1:1 relation.

It is also possible to specify B<JOINs>. Here's how:

  *set = DBIx::Recordset -> Search ({
            '!DataSource' => 'dbi:drv:db',
	    '!Table'   => 'name, street',
	    '!TabJoin' => 'name LEFT JOIN street ON (name.street_id=street.id)'}) ;


The difference between this and the first example is that this version 
also returns a record even if neither table contains a record for the 
given id. The way it's done depends on the JOIN you are given (LEFT/RIGHT/INNER) 
(see your SQL documentation for details about JOINs).

=head2 Links

If you have 1:n relations between two tables, the following may be a better
way to handle it:

  *set = DBIx::Recordset -> Search ({'!DataSource' => 'dbi:drv:db',
		     '!Table'	   => 'name',
		     '!Links'	   => {
			'-street'  => {
			    '!Table' => 'street',
			    '!LinkedField' => 'id',
			    '!MainField'   => 'street_id'
			    }
			}
		    }) ;

After that query, every record will contain the fields B<name> and B<street_id>.
Additionally, there is a pseudofield named B<-street>, which could be
used to access another recordset object, which is the result of a query
where B<street_id = id>. Use

  $set{name} to access the name field
  $set{-street}{street} to access the first street (as long as the
				    current record of the subobject isn't
				    modified)

  $set{-street}[0]{street}	first street
  $set{-street}[1]{street}	second street
  $set{-street}[2]{street}	third street

  $set[2]{-street}[1]{street} to access the second street of the
				    third name

You can have multiple linked tables in one recordset; you can also nest
linked tables or link a table to itself.


B<NOTE:> If you select only some fields and not all, the field which is specified by
'!MainField' must be also given in the '!Fields' or '$fields' parameter.

B<NOTE:> See also B<Automatic detection of links> below

=head2 LinkName

In the LinkName feature you may specify a "name" for every table. A name is one or 
more fields
which gives a human readable "key" of that record. For example in the above example 
B<id> is the
key of the record, but the human readable form is B<street>. 


  *set = DBIx::Recordset -> Search ({'!DataSource' => 'dbi:drv:db',
		     '!Table'	   => 'name',
		     '!LinkName'   => 1,
		     '!Links'	   => {
			'-street'  => {
			    '!Table' => 'street',
			    '!LinkedField' => 'id',
			    '!MainField'   => 'street_id',
			    '!NameField'   => 'street'
			    }
			}
		    }) ;

For every record in the table, this example will return the fields

  name  street_id  street

If you have more complex records, you may also specify more than one field in 
!NameField and pass it as an reference to an array e.g. ['street', 'city']. 
In this case, the result will contain

  name  street_id  street  city

If you set !LinkName to 2, the result will contain the fields

  name  street_id  STREET_ID

where STREET_ID contains the values of the street and city fields joined together. If you 
set !LinkName
to 3, you will get only

  name  street_id

where street_id contains the values of the street and city fields joined together. 


NOTE: The !NameField can also be specified as a table attribute with the function 
TableAttr. In this
case you don't need to specify it in every link. When a !NameField is given in a link 
description,
it overrides the table attribute.


=head2 Automatic detection of links

DBIx::Recordset and DBIx::Database will try to automatically detect links between tables
based on the field and table names. For this feature to work, the field which points to 
another table must consist of the table name and the field name of the destination joined
together with an underscore (as in the above example name.street_id). Then it will 
automatically recognized as a pointer to street.id.

  *set = DBIx::Recordset -> Search ({'!DataSource' => 'dbi:drv:db',
				     '!Table'	   => 'name') ;

is enough. DBIx::Recordset will automatically add the !Links attribute. You may use the
!Links attribute to specify links which can not be automatically detected.


=head1 DBIx::Database

The DBIx::Database object gathers information about a datasource. Its main purpose is 
to create, at startup, an object which retrieves all necessary information from the 
database; tries to detect links between tables; and stores this information for use 
by the DBIx::Recordset objects. There are additional methods which allow you to add kinds 
of information which cannot be retreived automatically.

=head2 new ($data_source, $username, $password, \%attr, $saveas, $keepopen)

=over 4

=item $data_source

Specifies the database to which to connect. 
Driver/DB/Host. Same as the first parameter to the DBI connect function.

=item $username

Username (optional)

=item $password

Password (optional) 

=item \%attr 

Attributes (optional) Same as the attribute parameter to the DBI connect function.

=item $saveas

Name for this DBIx::Database object to save as.
The name can be used in DBIx::Database::Get, or as !DataSource parameter in call to the
DBIx::Recordset object.

This is intended as mechanisem to retrieve the necessary metadata; for example, when 
your web server starts (e.g. in the startup.pl file of mod_perl). 
Here you can give the database
object a name. Later in your mod_perl or Embperl scripts, you can use this metadata by
specifying this name. This will speed up the setup of DBIx::Recordset object without the 
need to pass a reference to the DBIx::Database object.


=item $keepopen

Normaly the database connection will be closed after the metadata has been retrieved from
the database. This makes sure you don't get trouble when using the new method in a mod_perl
startup file. You can keep the connection open to use them in further setup call to DBIx::Recordset
objects.


=back




=head2 $db = DBIx::Database -> Get ($name)

$name = The name of the DBIx::Database object you wish to retrieve 


Get a DBIx::Database object which has already been set up based on the name



=head2 $db -> TableAttr ($table, $key, $value)

get and/or set an attribute for an specfic table

=over 4

=item $table

Name of table(s)

=item $key

key to set/get

=item $value

if present, set key to this value

=back

=head2 $db -> TableLink ($table, $linkname, $value)

Get and/or set a link description for an table

=over 4

=item $table

Name of table(s)

=item $linkname

Name of link to set/get

=item $value

if present, this must be a reference to a hash with the link decription.
See !Links for more information.

=back

=head2 $db -> AllTables

This returns a reference to a hash of the keys to all the tables of 
the datasource.




=head1 DEBUGING

DBIx::Recordset is able to write a logfile, so you can see what's happening
inside. There are two public variables used for this purpose:

=over 4

=item $DBIx::Recordset::Debug

Debuglevel 
 0 = off
 1 = log only errors
 2 = show connect, disconnect and SQL Statements
 3 = some more infos 
 4 = much infos

=item DBIx::Recordset::LOG

The filehandle used for logging. The default is STDERR, unless you are running under 
HTML::Embperl, in which case the default is the Embperl logfile.

=back

 Example:

    # open the log file
    open LOG, ">test.log" or die "Cannot open test.log" ; 

    # assign filehandle
    *DBIx::Recordset::LOG = \*LOG ; 
    
    # set debugging level
    $DBIx::Recordset::Debug = 2 ; 

    # now you can create a new DBIx::Recordset object



=head1 SECURITY

Since one possible application of DBIx::Recordset is its use in a web-server
environment, some attention should paid to security issues.

The current version of DBIx::Recordset does not include extended security management, 
but some features can be used to make your database access safer. (more security features
will come in further releases).

First of all, use the security feature of your database. Assign the web server
process as few rights as possible.

The greatest security risk is when you feed DBIx::Recordset a hash which 
contains the formfield data posted to the web server. Somebody who knows DBIx::Recordset
can post other parameters than those you would expect a normal user to post. For this 
reason, a primary issue is to override all parameters which should B<never> be posted by 
your script.

Example:
 *set = DBIx::Recordset -> Search ({%fdat,
				                    ('!DataSource'	=>  
"dbi:$Driver:$DB",
				                     '!Table'	=>  "$Table")}) ;

(assuming your posted form data is in %fdat). The above call will make sure
that nobody from outside can override the values supplied by $Driver, $DB and
$Table.

It is also wise to pre-setup your objects by supplying parameters
which can not be changed. 

Somewhere in your script startup (or at server startup time) add a setup call:

 *set = DBIx::Recordset-> setup ({'!DataSource'  =>  "dbi:$Driver:$DB",
			                        '!Table'	  =>  "$Table",
			                        '!Fields'	  =>  "a, b, c"}) ;

Later, when you process a request you can write:

 $set -> Search (\%fdat) ;

This will make sure that only the database specified by $Driver, $DB, the
table specified by $Table and the Fields a, b, and c can be accessed.


=head1 Compatibility with different DBD drivers

I have put a great deal of effort into making DBIx::Recordset run with various DBD drivers.
The problem is that not all necessary information is specified via the DBI interface (yet).
So I have made the module B<DBIx::Compat> which gives information about the 
difference between various DBD drivers and their underlying database systems. 
Currently, there are definitions for:

=item B<DBD::mSQL>

=item B<DBD::mysql>

=item B<DBD::Pg>

=item B<DBD::Solid>

=item B<DBD::ODBC>

=item B<DBD::CSV>

=item B<DBD::Oracle (requires DBD::Orcale 0.60 or higher)>

=item B<DBD::Sysbase (not fully tested)>

DBIx::Recordset has been tested with all those DBD drivers (on Linux 2.0.32, except 
DBD::ODBC, which has been tested on Windows '95 using Access 7).


If you want to use another DBD driver with DBIx::Recordset, it may
be necessary to create an entry for that driver. 
See B<perldoc DBIx::Compat> for more information.





=head1 EXAMPLES

The following are some examples of how to use DBIx::Recordset. The Examples are
from the test.pl. The examples show the DBIx::Recordset call first, followed by the
generated SQL command.


 *set = DBIx::Recordset-> setup ({'!DataSource'  =>  "dbi:$Driver:$DB",
                    			    '!Table'	  =>  "$Table"}) ;

Setup a DBIx::Recordset for driver $Driver, database $DB to access table $Table.


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
 B<Note:> Because of the B<start> and B<max> only records 0,1 will be returned


 $set1 -> Search ({'$start'=>0,'$max'=>2, '$next'=>1, '$order'=>'id'})  or die "not ok 
($DBI::errstr)" ;

 SELECT * form <table> ORDER BY id ;
 B<Note:> Because of the B<start>, B<max> and B<next> only records 2,3 will be 
returned


 $set1 -> Search ({'$start'=>2,'$max'=>1, '$prev'=>1, '$order'=>'id'})  or die "not ok 
($DBI::errstr)" ;

 SELECT * form <table> ORDER BY id ;
 B<Note:> Because of the B<start>, B<max> and B<prev> only records 0,1,2 will be 
returned


 $set1 -> Search ({'$start'=>5,'$max'=>5, '$next'=>1, '$order'=>'id'})  or die "not ok 
($DBI::errstr)" ;

 SELECT * form <table> ORDER BY id ;
 B<Note:> Because of the B<start>, B<max> and B<next> only records 5-9 will be 
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
 SELECT id, name, text FROM t1, t2 WHERE (name = 'Fourth Name') and 
t1.value=t2.value 
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

 SELECT id, name, text FROM t1, t2 WHERE (t1.value=t2.value) and (  ((name <> 
Fourth 
Name)) and (  (  id < 7  or  addon = 7)  or  (  id < 0  or  addon = 0)))


 $set6 -> Search ({'+id|addon'     =>  "6\tit",
                  'name'          =>  'Fourth Name',
                  '*id'           =>  '>',
                  '*addon'        =>  '<>',
                  '*name'         =>  '=',
                  '$compconj'     =>  'and',
                  '$conj'         =>  'or' }) or die "not ok ($DBI::errstr)" ;


 SELECT id, name, text FROM t1, t2 WHERE (t1.value=t2.value) and (  ((name = 
Fourth 
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

As far as possible for me, support will be available via the DBI Users' mailing 
list. (dbi-user@fugue.com)

=head1 AUTHOR

G.Richter (richter@dev.ecos.de)

=head1 SEE ALSO

=item Perl(1)
=item DBI(3)
=item DBIx::Compat(3)
=item HTML::Embperl(3) 
http://perl.apache.org/embperl/
=item Tie::DBI(3)
http://stein.cshl.org/~lstein/Tie-DBI/


=cut


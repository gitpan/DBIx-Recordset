
###################################################################################
#
#   DBIx::Recordset - Copyright (c) 1997-2000 Gerald Richter / ECOS
#
#   You may distribute under the terms of either the GNU General Public
#   License or the Artistic License, as specified in the Perl README file.
#
#   THIS IS BETA SOFTWARE!
#
#   THIS PACKAGE IS PROVIDED "AS IS" AND WITHOUT ANY EXPRESS OR
#   IMPLIED WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED
#   WARRANTIES OF MERCHANTIBILITY AND FITNESS FOR A PARTICULAR PURPOSE.
#
#   $Id: Recordset.pm,v 1.73 2000/09/22 05:34:29 richter Exp $
#
###################################################################################


package DBIx::Database::Base ;

use strict 'vars' ;

use vars qw{$LastErr $LastErrstr *LastErr *LastErrstr *LastError $PreserveCase} ;

*LastErr        = \$DBIx::Recordset::LastErr ;
*LastErrstr     = \$DBIx::Recordset::LastErrstr ;
*LastError      = \&DBIx::Recordset::LastError ;
*PreserveCase   = \$DBIx::Recordset::PreserveCase;


use Carp ;

## ----------------------------------------------------------------------------
##
## savecroak
##
## croaks and save error
##


sub savecroak

    {
    my ($self, $msg, $code) = @_ ;

    $LastErr	= $self->{'*LastErr'}	    = $code || $dbi::err || -1 ;
    $LastErrstr = $self->{'*LastErrstr'}    = $msg || $DBI::errstr || ("croak from " . caller) ;

    #$Carp::extra = 1 ;
    #Carp::croak $msg ;
    Carp::confess $msg ;
    }

## ----------------------------------------------------------------------------
##
## DoOnConnect
##
## in $cmd  sql cmds
##

sub DoOnConnect

    {
    my ($self, $cmd) = @_ ;
    
    if ($cmd)
        {
        if (ref ($cmd) eq 'ARRAY')
            {
            foreach (@$cmd)
                {
                $self -> do ($_) ;
                }
            }
        elsif (ref ($cmd) eq 'HASH')
            {
            $self -> DoOnConnect ($cmd -> {'*'}) ;
            $self -> DoOnConnect ($cmd -> {$self -> {'*Driver'}}) ;
            }
        else
            {
            $self -> do ($cmd) ;
            }
        }
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
## do an non select statement 
##
## $statement = statement to do
## \%attr     = attribs (optional)
## @bind_valus= values to bind (optional)
## or 
## \@bind_valus= values to bind (optional)
## \@bind_types  = data types of bind_values
##

sub do($$;$$$)

    {
    my($self, $statement, $attribs, @params) = @_;
    
    $self -> {'*LastSQLStatement'} = $statement ;

    my $ret ;
    my $bval ;
    my $btype ;
    my $dbh ;
    my $sth ;

    if (@params > 1 && ref ($bval = $params[0]) eq 'ARRAY' && ref ($btype = $params[1]) eq 'ARRAY')
        {
        if ($self->{'*Debug'} > 1) { local $^W = 0 ; print DBIx::Recordset::LOG "DB:  do '$statement' bind_values=<@$bval> bind_types=<@$btype>\n" } ;
        $dbh = $self->{'*DBHdl'} ;
        $sth = $dbh -> prepare ($statement, $attribs) ;
        my $Numeric = $self->{'*NumericTypes'} || {} ;
        local $^W = 0 ; # avoid warnings
        if (defined ($sth))
            {
            for (my $i = 0 ; $i < @$bval; $i++)
                {
                $bval -> [$i] += 0 if (defined ($bval -> [$i]) && defined ($btype -> [$i]) && $Numeric -> {$btype -> [$i]}) ;
                #$sth -> bind_param ($i+1, $bval -> [$i], $btype -> [$i]) ;
                #$sth -> bind_param ($i+1, $bval -> [$i], $btype -> [$i] == DBI::SQL_CHAR()?DBI::SQL_CHAR():undef ) ;
		my $bt = $btype -> [$i] ;
                $sth -> bind_param ($i+1, $bval -> [$i], (defined ($bt) && $bt <= DBI::SQL_CHAR())?{TYPE=>$bt}:undef ) ;
                }
            $ret = $sth -> execute ;
            }
        }
    else
        {
        print DBIx::Recordset::LOG "DB:  do $statement <@params>\n" if ($self->{'*Debug'} > 1) ;
        
        $ret = $self->{'*DBHdl'} -> do ($statement, $attribs, @params) ;
        }

    print DBIx::Recordset::LOG "DB:  do returned " . (defined ($ret)?$ret:'<undef>') . "\n" if ($self->{'*Debug'} > 2) ;
    print DBIx::Recordset::LOG "DB:  ERROR $DBI::errstr\n"  if (!$ret && $self->{'*Debug'}) ;
    print DBIx::Recordset::LOG "DB:  in do $statement <@params>\n" if (!$ret && $self->{'*Debug'} == 1) ;

    $LastErr	= $self->{'*LastErr'}	    = $DBI::err ;
    $LastErrstr = $self->{'*LastErrstr'}    = $DBI::errstr ;
    
    return $ret ;
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
            
    $table = lc($table)  if (!$PreserveCase) ;

    my $meta ;
    my $metakey    = "$self->{'*DataSource'}//" . $table ;
    
    if (defined ($meta = $DBIx::Recordset::Metadata{$metakey})) 
        {
        print DBIx::Recordset::LOG "DB:   use cached meta data for $table\n" if ($self->{'*Debug'} > 2) ;
        return $meta 
        }

    my $hdl = $self->{'*DBHdl'} ;
    my $drv = $self->{'*Driver'} ;
    my $sth ;
    
    my $ListFields = DBIx::Compat::GetItem ($drv, 'ListFields') ;
    my $QuoteTypes = DBIx::Compat::GetItem ($drv, 'QuoteTypes') ;
    my $NumericTypes = DBIx::Compat::GetItem ($drv, 'NumericTypes') ;
    my $HaveTypes  = DBIx::Compat::GetItem ($drv, 'HaveTypes') ;
    my @tabs = split (/\s*\,\s*/, $table) ;
    my $tab ;
    my $ltab ;
    my %Quote ;
    my %Numeric ;
    my @Names ;
    my @Types ;
    my @FullNames ;
    my %Table4Field ;
    my %Type4Field ;
    my $i ;

    foreach $tab (@tabs)
        {
        next if ($tab =~ /^\s*$/) ;
    
        $sth = &{$ListFields}($hdl, $tab) or carp ("Cannot list fields for $tab ($DBI::errstr)") ;
	$ltab = $tab ;
	
        my $types ;
        my $fields = $sth?$sth -> FETCH ($PreserveCase?'NAME':'NAME_lc'):[]  ;
        my $num = $#{$fields} + 1 ;
    
        if ($HaveTypes && $sth)
            {
            #print DBIx::Recordset::LOG "DB: Have Types for driver\n" ;
            $types = $sth -> FETCH ('TYPE')  ;
            }
        else
            {
            #print DBIx::Recordset::LOG "DB: No Types for driver\n" ;
            # Drivers does not have fields types -> give him SQL_VARCHAR
            $types = [] ;
            for ($i = 0; $i < $num; $i++)
                { push @$types, DBI::SQL_VARCHAR (); }

            # Setup quoting for SQL_VARCHAR
            $QuoteTypes = { DBI::SQL_VARCHAR() => 1 } ;
            $NumericTypes = { } ;
            }
    
        push @Names, @$fields ;
        push @Types, @$types ;
        $i = 0 ;
        foreach (@$fields)
            {
	    $Table4Field{$_}         = $ltab ;        
            $Table4Field{"$ltab.$_"} = $ltab ;
            $Type4Field{"$_"}        = $types -> [$i] ;
            $Type4Field{"$ltab.$_"}  = $types -> [$i++] ;
            push @FullNames, "$ltab.$_"  ;
            }        

        $sth -> finish if ($sth) ;

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
                print DBIx::Recordset::LOG "DB: TAB = $tab, COL = $col, NAME = $n, TYPE = $t" ;
                }
            $fieldname = $$fields[$col] ;
            if ($$QuoteTypes{$$types[$col]})
                {
                #print DBIx::Recordset::LOG " -> quote\n" if ($self->{'*Debug'} > 2) ;
                $Quote {"$tab.$fieldname"} = 1 ;
                $Quote {"$fieldname"} = 1 ;
                }
            else
                {
                #print DBIx::Recordset::LOG "\n" if ($self->{'*Debug'} > 2) ;
                $Quote {"$tab.$fieldname"} = 0 ;
                $Quote {"$fieldname"} = 0 ;
                }
            if ($$NumericTypes{$$types[$col]})
                {
                print DBIx::Recordset::LOG " -> numeric\n" if ($self->{'*Debug'} > 2) ;
                $Numeric {"$tab.$fieldname"} = 1 ;
                $Numeric {"$fieldname"} = 1 ;
                }
            else
                {
                print DBIx::Recordset::LOG "\n" if ($self->{'*Debug'} > 2) ;
                $Numeric {"$tab.$fieldname"} = 0 ;
                $Numeric {"$fieldname"} = 0 ;
                }
            }
        print DBIx::Recordset::LOG "No Fields found for $tab\n" if ($num == 0 && $self->{'*Debug'} > 1) ;
        }

    print DBIx::Recordset::LOG "No Tables specified\n" if ($#tabs < 0 && $self->{'*Debug'} > 1) ;


    $meta = {} ;
    $meta->{'*Table4Field'}  = \%Table4Field ;
    $meta->{'*Type4Field'}   = \%Type4Field ;
    $meta->{'*FullNames'}    = \@FullNames ;
    $meta->{'*Names'}  = \@Names ;
    $meta->{'*Types'}  = \@Types ;
    $meta->{'*Quote'}  = \%Quote ;    
    $meta->{'*Numeric'}  = \%Numeric ;    
    $meta->{'*NumericTypes'}  = $NumericTypes ;    

    $DBIx::Recordset::Metadata{$metakey} = $meta ;
    

    if (!exists ($meta -> {'*Links'}))
        { 
        my $ltab ;
        my $lfield ;
        my $metakey ;
        my $subnames ;
        my $n ;

        $meta -> {'*Links'} = {} ;

        my $metakeydsn = "$self->{'*DataSource'}//-" ;
        my $metakeydsntf = "$self->{'*DataSource'}//-"  . ($self->{'*TableFilter'}||'');
        my $metadsn    = $DBIx::Recordset::Metadata{$metakeydsn} || {} ;
        my $tabmetadsn = $DBIx::Recordset::Metadata{$metakeydsntf} || {} ;
        my $tables     = $tabmetadsn -> {'*Tables'} ;

        if (!$tables)
            { # Query the driver, which tables are available
            my $ListTables = DBIx::Compat::GetItem ($drv, 'ListTables') ;

	    if ($ListTables)
		{            
		my @tabs = &{$ListTables}($hdl) or $self -> savecroak ("Cannot list tables for $self->{'*DataSource'} ($DBI::errstr)") ;
		my @stab ;
		my $stab ;
                my $tabfilter = $self -> {'*TableFilter'} || '.' ;
                foreach (@tabs)
                    {
                    if ($_ =~ /(^|\.)$tabfilter/i)
                        {
                        @stab = split (/\./);
                        $stab = $PreserveCase?(pop @stab):lc (pop @stab) ;
                        $tables -> {$stab} =  $_ ;
                        }
                    }
		$tabmetadsn -> {'*Tables'} = $tables ;
		if ($self->{'*Debug'} > 3) 
		    {
		    my $t ;
		    foreach $t (keys %$tables)
			{ print DBIx::Recordset::LOG "DB:  Found table $t => $tables->{$t}\n" ; }
		    }
		}
	    else
		{
		$tabmetadsn -> {'*Tables'} = {} ;
		}
            
            $DBIx::Recordset::Metadata{$metakeydsn} = $metadsn ;
            $DBIx::Recordset::Metadata{"$metakeydsn$self->{'*TableFilter'}"} = $tabmetadsn if ($self->{'*TableFilter'}) ;
            }

	if ($#tabs <= 0)
	    {
	    my $fullname ;
            my $tabfilter = $self -> {'*TableFilter'}  ;
	    my $fullltab ;
            my $tableshort = $table ;
            if ($tabfilter && ($table =~ /^$tabfilter(.*?)$/))
                {
                $tableshort     = $1 ;
                }
            foreach $fullname (@FullNames)
		{
		my ($ntab, $n) = split (/\./, $fullname) ;
		my $prefix = '' ;
                my $fullntab = $ntab ;
                
                if ($tabfilter && ($ntab =~ /^$tabfilter(.*?)$/))
                    {
                    $ntab     = $1 ;
                    }

		if ($n =~ /^(.*?)__(.*?)$/)
		    {
		    $prefix = "$1__" ;
		    $n = $2 ;
		    }

		my @part = split (/_/, $n) ;
		my $tf = $tabfilter || '' ;
                for (my $i = 0; $i < $#part; $i++)
		    {
		    $ltab   = join ('_', @part[0..$i]) ;
		    $lfield = join ('_', @part[$i + 1..$#part]) ;
            
		    next if (!$ltab) ;
                    
                    if (!$tables -> {$ltab} && $tables -> {"$tf$ltab"}) 
                        { $fullltab = "$tabfilter$ltab" }
                    else
                        { $fullltab = $ltab }

		    if ($tables -> {$fullltab}) 
			{
			$metakey = $self -> QueryMetaData ($fullltab) ;
			$subnames = $metakey -> {'*Names'} ;
			if (grep (/^$lfield$/i, @$subnames))
			    { # setup link
			    $meta -> {'*Links'}{"-$prefix$ltab"} = {'!Table' => $fullltab, '!LinkedField' => $lfield, '!MainField' => "$prefix$n", '!MainTable' => $fullntab} ;
			    print DBIx::Recordset::LOG "Link found for $ntab.$prefix$n to $ltab.$lfield\n" if ($self->{'*Debug'} > 2) ;
                        
			    #my $metakeyby    = "$self->{'*DataSource'}//$ltab" ;
			    #my $linkedby = $DBIx::Recordset::Metadata{$metakeyby} -> {'*Links'} ;
			    my $linkedby = $metakey -> {'*Links'} ;
			    my $linkedbyname = "\*$prefix$tableshort" ;
                            $linkedby -> {$linkedbyname} = {'!Table' => $fullntab, '!MainField' => $lfield, '!LinkedField' => "$prefix$n", '!LinkedBy' => $fullltab, '!MainTable' => $fullltab} ;
			    #$linkedby -> {"-$tableshort"} = $linkedby -> {$linkedbyname} if (!exists ($linkedby -> {"-$tableshort"})) ;
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
                next if (!$ltab) ;
                $metakey = $self -> QueryMetaData ($ltab) ;

		my $k ;
		my $v ;
		my $lbtab ;
		my $links = $metakey -> {'*Links'} ;
		while (($k, $v) = each (%$links))
		    {
		    if (!$meta -> {'*Links'}{$k}) 
			{
			$meta -> {'*Links'}{$k} = { %$v } ;
    			print DBIx::Recordset::LOG "Link copied: $k\n" if ($self->{'*Debug'} > 2) ;
			}
		    
		    }
		}
	    }

	}


    return $meta ;
    }


###################################################################################

package DBIx::Database ;

use strict 'vars' ;

use vars qw{$LastErr $LastErrstr *LastErr *LastErrstr *LastError $PreserveCase @ISA} ;

@ISA = ('DBIx::Database::Base') ;

*LastErr    = \$DBIx::Recordset::LastErr ;
*LastErrstr = \$DBIx::Recordset::LastErrstr ;
*LastError  = \&DBIx::Recordset::LastError ;
*PreserveCase  = \$DBIx::Recordset::PreserveCase;


use Carp ;

## ----------------------------------------------------------------------------
##
## connect
##

sub connect

    {
    my ($self, $password) = @_ ; 

    my $hdl = $self->{'*DBHdl'}  = DBI->connect($self->{'*DataSource'}, $self->{'*Username'}, $password, $self->{'*DBIAttr'}) or $self -> savecroak ("Cannot connect to $self->{'*DataSource'} ($DBI::errstr)") ;

    $LastErr    = $self->{'*LastErr'}	    = $DBI::err ;
    $LastErrstr = $self->{'*LastErrstr'}    = $DBI::errstr ;

    $self->{'*MainHdl'}    = 1 ;
    $self->{'*Driver'}     = $hdl->{Driver}->{Name} ;
    if ($self->{'*Driver'} eq 'Proxy')
	{
        $self->{'*DataSource'} =~ /dsn\s*=\s*dbi:(.*?):/i ;
	$self->{'*Driver'} = $1 ;
	print DBIx::Recordset::LOG "DB:  Found DBD::Proxy, take compability entrys for driver $self->{'*Driver'}\n" if ($self->{'*Debug'} > 1) ;
	}

    print DBIx::Recordset::LOG "DB:  Successfull connect to $self->{'*DataSource'} \n" if ($self->{'*Debug'} > 1) ;

    my $cmd ;
    if ($hdl && ($cmd = $self -> {'*DoOnConnect'}))
        {
        $self -> DoOnConnect ($cmd) ;
        }
  
    return $hdl ;
    }


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
## $tabfilter    = regex which tables should be used
##

sub new

    {
    my ($class, $data_source, $username, $password, $attr, $saveas, $keepopen, $tabfilter, $doonconnect) = @_ ;
    

    if (ref ($data_source) eq 'HASH')
        {
        my $p = $data_source ;
        ($data_source, $username, $password, $attr, $saveas, $keepopen, $tabfilter, $doonconnect) = 
        @$p{('!DataSource', '!Username', '!Password', '!DBIAttr', '!SaveAs', '!KeepOpen', '!TableFilter', '!DoOnConnect')} ;
        }
            
    
    my $metakey  ;
    my $self ;

    if (!($data_source =~ /^dbi:/i)) 
        {
        $metakey    = "-DATABASE//$1"  ;
        $self = $DBIx::Recordset::Metadata{$metakey} ;
        $self -> connect ($password) if ($keepopen && !defined ($self->{'*DBHdl'})) ;
        return $self ;
        }
    
    if ($saveas)
        {
        $metakey    = "-DATABASE//$saveas"  ;
        if (defined ($self = $DBIx::Recordset::Metadata{$metakey}))
            {
            $self -> connect ($password) if ($keepopen && !defined ($self->{'*DBHdl'})) ;
            return $self ;
            }
        }


    $self = {
                '*Debug'      => $DBIx::Recordset::Debug,
                '*DataSource' => $data_source,
                '*DBIAttr'    => $attr,
                '*Username'   => $username, 
                '*TableFilter' => $tabfilter, 
                '*DoOnConnect' => $doonconnect,
               } ;

    bless ($self, $class) ;

    my $hdl ;

    if (!defined ($self->{'*DBHdl'}))
        {
        $hdl = $self->connect ($password) ;
        }
    else
        {
        $LastErr	= $self->{'*LastErr'}   = undef ;
        $LastErrstr = $self->{'*LastErrstr'}    = undef ;
    
        $hdl = $self->{'*DBHdl'} ;
        print DBIx::Recordset::LOG "DB:  Use already open dbh for $self->{'*DataSource'}\n" if ($self->{'*Debug'} > 1) ;
        }
            
    $DBIx::Recordset::Metadata{"$self->{'*DataSource'}//*"} ||= {} ; # make sure default table is defined

    my $drv        = $self->{'*Driver'} ;
    my $metakeydsn = "$self->{'*DataSource'}//-" ;
    my $metakeydsntf = "$self->{'*DataSource'}//-"  . ($self->{'*TableFilter'}||'');
    my $metadsn    = $DBIx::Recordset::Metadata{$metakeydsn} || {} ;
    my $tabmetadsn = $DBIx::Recordset::Metadata{$metakeydsntf} || {} ;
    my $tables     = $tabmetadsn -> {'*Tables'} ;

    if (!$tables)
        { # Query the driver, which tables are available
        my $ListTables = DBIx::Compat::GetItem ($drv, 'ListTables') ;

        
        if ($ListTables)
	    {
	    my @tabs = &{$ListTables}($hdl) ; # or $self -> savecroak ("Cannot list tables for $self->{'*DataSource'} ($DBI::errstr)") ;
	    my @stab ;
	    my $stab ;

            $tabfilter ||= '.' ;
            foreach (@tabs)
                {
                if ($_ =~ /(^|\.)$tabfilter/i)
                    {
                    @stab = split (/\./);
                    $stab = $PreserveCase?(pop @stab):lc (pop @stab) ;
                    $tables -> {$stab} =  $_ ;
                    }
                }
        
	    $tabmetadsn -> {'*Tables'} = $tables ;
	    if ($self->{'*Debug'} > 2) 
		{
		my $t ;
		foreach $t (keys %$tables)
		    { print DBIx::Recordset::LOG "DB:  Found table $t => $tables->{$t}\n" ; }
		}
	    }
	else    
	    {
	    $tabmetadsn -> {'*Tables'} = {} ;
	    }
            
        $DBIx::Recordset::Metadata{$metakeydsn} = $metadsn ;
        $DBIx::Recordset::Metadata{$metakeydsntf} = $tabmetadsn ;
        }

    my $tab ;
    my $x ;

    while (($tab, $x) = each (%{$tables}))
        {
        $self -> QueryMetaData ($tab) ;
        }

    
    $DBIx::Recordset::Metadata{$metakey} = $self if ($metakey) ;

    # disconnect in case we are running in a Apache/mod_perl startup file
    
    if (defined ($self->{'*DBHdl'}) && !$keepopen)
        {
        $self->{'*DBHdl'} -> disconnect () ;
        undef $self->{'*DBHdl'} ;
        print DBIx::Recordset::LOG "DB:  Disconnect from $self->{'*DataSource'} \n" if ($self->{'*Debug'} > 1) ;
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

    $table = lc($table) if (!$PreserveCase) ;

    my $meta ;
    my $metakey    = "$self->{'*DataSource'}//$table" ;
    
    if (!defined ($meta = $DBIx::Recordset::Metadata{$metakey})) 
        {
        $self -> savecroak ("Unknown table $table in $self->{'*DataSource'}") ;
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

    $table = lc($table)  if (!$PreserveCase) ;

    my $meta ;
    my $metakey    = "$self->{'*DataSource'}//$table" ;
    
    if (!defined ($meta = $DBIx::Recordset::Metadata{$metakey})) 
        {
        $self -> savecroak ("Unknown table $table in $self->{'*DataSource'}") ;
        }

    return $meta -> {'*Links'} if (!defined ($key)) ;

    return $meta -> {'*Links'} -> {$key} = $value if (defined ($value)) ;

    return $meta -> {'*Links'} -> {$key}  ;
    }


## ----------------------------------------------------------------------------
##
## MetaData
##
## get/set metadata for a given table
##
## $table     = Name of table
## $metadata  = meta data to set
##


sub MetaData

    {
    my ($self, $table, $metadata, $clear) = @_ ;

    $table = lc($table)  if (!$PreserveCase) ;

    my $meta ;
    my $metakey    = "$self->{'*DataSource'}//$table" ;
    
    if (!defined ($meta = $DBIx::Recordset::Metadata{$metakey})) 
        {
        $self -> savecroak ("Unknown table $table in $self->{'*DataSource'}") ;
        }

    return $meta if (!defined ($metadata) && !$clear) ;

    return $DBIx::Recordset::Metadata{$metakey} = $metadata ;
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
    my $metakeydsn = "$self->{'*DataSource'}//-" . ($self->{'*TableFilter'} || '') ;
    my $metadsn    = $DBIx::Recordset::Metadata{$metakeydsn} || {} ;
    return $metadsn -> {'*Tables'} ;
    }

## ----------------------------------------------------------------------------
##
## AllNames
##
## return reference to array of all names in all tables
##
## $table     = Name of table
##

sub AllNames

    {
    my ($self, $table) = @_ ;

    $table = lc($table)  if (!$PreserveCase) ;

    my $meta ;
    my $metakey    = "$self->{'*DataSource'}//$table" ;
    
    if (!defined ($meta = $DBIx::Recordset::Metadata{$metakey})) 
        {
        $self -> savecroak ("Unknown table $table in $self->{'*DataSource'}") ;
        }

    return $meta -> {'*Names'}  ;
    }

## ----------------------------------------------------------------------------
##
## AllTypes
##
## return reference to array of all types in all tables
##
## $table     = Name of table
##

sub AllTypes

    {
    my ($self, $table) = @_ ;

    $table = lc($table)  if (!$PreserveCase) ;

    my $meta ;
    my $metakey    = "$self->{'*DataSource'}//$table" ;
    
    if (!defined ($meta = $DBIx::Recordset::Metadata{$metakey})) 
        {
        $self -> savecroak ("Unknown table $table in $self->{'*DataSource'}") ;
        }

    return $meta -> {'*Types'}  ;
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
    my $orgerr = $@ ;
    local $@ ;

    eval 
	{ 
	if (defined ($self->{'*DBHdl'}))
	    {
	    $self->{'*DBHdl'} -> disconnect () ;
	    undef $self->{'*DBHdl'} ;
	    }
	} ;
    $self -> savecroak ($@) if (!$orgerr && $@) ;
    warn $@ if ($orgerr && $@) ;
    }


###################################################################################

1;
__END__

=pod

=head1 NAME

DBIx::Database / DBIx::Recordset - Perl extension for DBI recordsets

=head1 SYNOPSIS

 use DBIx::Database;

=head1 DESCRIPTION

See perldoc DBIx::Recordset for an description.

=head1 AUTHOR

G.Richter (richter@dev.ecos.de)


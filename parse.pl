#!/usr/bin/perl
use strict;

my $mode = $ARGV[0]=~/^(intf|impl)$/ ? $1 : die;
my @lines = <STDIN>;
s{//.*}{} for @lines;
my $src = join ' ', @lines;

my $chk_unparsed = sub{ /\S/ and die "unparsed ($_)" for @_ };

my $gen_intf = sub{
    my(%message)=@_;
    my $args = join ', ', map{
        "$$_{name}: $$_{resultType} = $$_{empty}"
    } @{$message{fields}||[]};
    "case class $message{name}($args)\n"
};

my $gen_impl = sub{
        my(%message)=@_;
        my $size = join '', map{
            "\n                value.$$_{name}.foreach(item => res += $$_{serde}.encodedSizeWithTag($$_{id}, item))"
        } @{$message{fields}||[]};
        my $encode = join '', map{
            "\n                value.$$_{name}.foreach(item => $$_{serde}.encodeWithTag(writer, $$_{id}, item))"
        } @{$message{fields}||[]};
        my $nil = join '', map{
            "\n                var prep_$$_{name}: $$_{resultType} = $$_{empty}"
        } @{$message{fields}||[]};
        my $case = join '', map{
            $$_{repeated} ?
                "\n                    case $$_{id} => prep_$$_{name} = $$_{serde}.decode(reader) :: prep_$$_{name}" :
                "\n                    case $$_{id} => prep_$$_{name} = Option($$_{serde}.decode(reader))"
        } @{$message{fields}||[]};
        my $args = join ', ', map{
            "prep_$$_{name}"
        } @{$message{fields}||[]};
        "
        object $message{name}ProtoAdapter
            extends com.squareup.wire.ProtoAdapter[$message{name}](
                com.squareup.wire.FieldEncoding.LENGTH_DELIMITED,
                classOf[$message{name}]
            )
            with ProtoAdapterWithId
        {
            def id = $message{id}
            override def encodedSize(value: $message{name}): Int = {
                var res = 0
                $size
                res
            }
            override def encode(writer: com.squareup.wire.ProtoWriter, value: $message{name}) = {
                $encode
            }
            override def decode(reader: com.squareup.wire.ProtoReader) = {
                $nil
                val token = reader.beginMessage()
                var done = false
                while(!done) reader.nextTag() match {
                    case -1 => done = true
                    $case
                    case _ => reader.peekFieldEncoding.rawProtoAdapter.decode(reader)
                }
                reader.endMessage(token)
                $message{name}($args)
            }
        }"
};

my %resultType = (
    "sint32"  => "Int",
    "string"  => "String",
    "Decimal" => "BigDecimal",
    "Instant" => "java.time.Instant"
);

my $add_message = sub{
    my(%message)=@_;
    $message{body}=~s{\b(repeated\s+)?([A-Za-z]\w*)\s+([a-z]\w*)\s*=\s*(0x[0-9a-fA-F]{4})\s*;}{
        my %msg = (repeated=>"$1",type=>"$2",name=>"$3",id=>"$4");
        $msg{serde} = $msg{type}=~/^[a-z]/ ?
            "com.squareup.wire.ProtoAdapter.".uc($msg{type}) :
            "$msg{type}ProtoAdapter";
        $msg{empty} = $msg{repeated} ? "Nil" : "None";
        my $type = $resultType{$msg{type}} || $msg{type};
        $msg{resultType} = $msg{repeated} ? "List[$type]" : "Option[$type]";
        push @{$message{fields}||=[]}, \%msg;
        ""
    }egs;
    &$chk_unparsed(delete $message{body});
    \%message
};

#(val encode: $message{name}=>Array[Byte])\n"

my @messages;
$src=~s{\bmessage\s+([A-Z]\w*)\s+=\s+(0x[0-9a-fA-F]{4})\s*\{(.*?)\}\s*}{
    push @messages, &$add_message(name=>"$1",id=>"$2",body=>"$3");
    ""
}egs;
&$chk_unparsed($src);
if($mode eq 'intf'){
    print &$gen_intf(%$_) for @messages;
} elsif($mode eq 'impl'){
    my $impl = join '', map{&$gen_impl(%$_)} @messages;
    my $reg = join '', map{
        "\n                 $$_{name}ProtoAdapter ::"
    } @messages;
    print "
        $impl
        trait ProtoAdapterWithId {
            def id: Int
        }
        object ProtoAdapters {
            val list: List[com.squareup.wire.ProtoAdapter[_<:Object] with ProtoAdapterWithId] = $reg Nil
        }
    "
} else {
    die 'intf|impl mode must be in args';
}


=sk
Duration
Instant
Boolean
Decimal
=cut


using DataFrames

macro c(ret_type, func, arg_types, lib)
    local args_in = Any[ symbol(string('a',x)) for x in 1:length(arg_types.args) ]
    quote
        $(esc(func))($(args_in...)) = ccall( ($(string(func)), $(Expr(:quote, lib)) ),
                                            $ret_type, $arg_types, $(args_in...) )
    end
end

#stdlib stuff
@c Clong strtol (Ptr{UInt8}, Ptr{Ptr{UInt8}}, Cint) libc
@c Clonglong strtoll (Ptr{UInt8}, Ptr{Ptr{UInt8}}, Cint) libc
@c Cfloat strtof (Ptr{UInt8}, Ptr{Ptr{UInt8}}) libc
@c Cdouble strtod (Ptr{UInt8}, Ptr{Ptr{UInt8}}) libc

abstract AbstractPostgresType{T}

type PostgresType{T} <: AbstractPostgresType{T}
    name::Symbol
    naval::T
end

type PostgresEnumType{T <: UTF8String} <: AbstractPostgresType{T}
    name::Symbol
    naval::T
    enumvals::Set{T}
end

type PostgresDomainType{T} <: AbstractPostgresType{T}
    name::Symbol
    basetype::PostgresType{T}
end

function Base.show(io::IO, t::PostgresType)
        print(io, "$(t.name) -> $(typeof(t.naval))")
end

function Base.show(io::IO, t::PostgresEnumType)
        print(io, "$(t.name) ∈ $(t.enumvals)")
end

function Base.show(io::IO, t::PostgresDomainType)
        print(io, "($(t.name) <: $(t.basetype.name)) -> $(typeof(t.basetype.naval))")
end

naval(t::PostgresType) = t.naval
naval(t::PostgresEnumType) = t.naval
naval(t::PostgresDomainType) = t.basetype.naval

###########################################################################
### base parsers


function unsafe_parse(t::AbstractPostgresType, value::UTF8String)
    value
end

function unsafe_parse(t::AbstractPostgresType, ptr::Ptr{UInt8})
    unsafe_parse(t, utf8(ptr))
end

function unsafe_parse(t::PostgresDomainType, value::UTF8String)
    unsafe_parse(t.basetype, value)
end

###########################################################################
### c parsers

function unsafe_parse{T <: Int16}(::PostgresType{T}, ptr::Ptr{UInt8})
    convert(Int16, strtol(ptr, C_NULL, 10))
end

function unsafe_parse{T <: Int32}(::PostgresType{T}, ptr::Ptr{UInt8})
    strtol(ptr, C_NULL, 10)
end

function unsafe_parse{T <: Int64}(::PostgresType{T}, ptr::Ptr{UInt8})
    strtoll(ptr, C_NULL, 10)
end

function unsafe_parse{T <: Float32}(::PostgresType{T}, ptr::Ptr{UInt8})
    strtof(ptr, C_NULL)
end

function unsafe_parse{T <: Float64}(::PostgresType{T}, ptr::Ptr{UInt8})
    strtod(ptr, C_NULL)
end

###########################################################################
### julia parsers

#should not need with the c parsers
#function unsafe_parse{T <: Real}(::PostgresType{T}, value::UTF8String)
#    parse(T, value)
#end

function unsafe_parse{T <: Char}(t::PostgresType{T}, value::UTF8String)
    value == "" ? Char('\0') : Char(value[1])
end

function unsafe_parse{T <: BigFloat}(::PostgresType{T}, value::UTF8String)
    parse(BigFloat, value)
end

function unsafe_parse{T <: Date}(::PostgresType{T}, value::UTF8String)
    y, m, d = split(value, "-")
    Date(parse(Int, y), parse(Int, m), parse(Int, d))
end

function unsafe_parse{T <: Bool}(::PostgresType{T}, value::UTF8String)
    value == "t"
end

function unsafe_parse{T <: Vector{UInt8}}(::PostgresType{T}, value::UTF8String)
    hex2bytes(value[3:end])
end

function unsafe_parse{T <: BitVector}(::PostgresType{T}, value::UTF8String)
    BitVector([c=='1' for c in value])
end

#function unsafe_parse{T <: DateTime}(::PostgresType{T}, value::UTF8String)
#    DateTime(value[1:end-3], "y-m-d H:M:S.s")
#end

###########################################################################
### value converters

type PostgresValue{T}
    pgtype::AbstractPostgresType
    value::UTF8String
end
function Base.show(io::IO, val::PostgresValue)
    print(io, val.value)
end
function Base.showall(io::IO, val::PostgresValue)
    esc = val.pgtype.name == :bytea ? "e" : ""
    print(io, "$esc'$(val.value)'::$(val.pgtype.name)")
end

PostgresValue(val) =
    PostgresValue(base_types_byname[:text]utf8, utf8(string(val)))

PostgresValue{T <: AbstractString}(val::T) =
    PostgresValue{T}(base_types_byname[:text], utf8(string(val)))

PostgresValue{T <: Int16}(val::T) =
    PostgresValue{T}(base_types_byname[:int2], utf8(string(val)))

PostgresValue{T <: Int32}(val::T) =
    PostgresValue{T}(base_types_byname[:int4], utf8(string(val)))

PostgresValue{T <: Int64}(val::T) =
    PostgresValue{T}(base_types_byname[:int8], utf8(string(val)))

PostgresValue{T <: Float32}(val::T) =
    PostgresValue{T}(base_types_byname[:float4], utf8(string(val)))

PostgresValue{T <: Float64}(val::T) =
    PostgresValue{T}(base_types_byname[:float8], utf8(string(val)))

PostgresValue{T <: BigFloat}(val::T) =
    PostgresValue{T}(base_types_byname[:numeric], utf8(string(val)))

PostgresValue{T <: Date}(val::T) =
    PostgresValue{T}(base_types_byname[:date], utf8(string(val)))

PostgresValue{T <: Bool}(val::T) =
    PostgresValue{T}(base_types_byname[:bool], utf8(string(val)))
 
function PostgresValue{T <: Vector{UInt8}}(val::T)
    v = "\\\\x" * join(map(x -> repr(x)[3:end], val))
    PostgresValue{T}(base_types_byname[:bytea], v)
end

function PostgresValue{T <: BitVector}(val::T)
    v = join(map(x -> x ? "1" : "0", val))
    PostgresValue{T}(base_types_byname[:varbit], v)
end

function pgtypes(df::DataFrame)
    a = AbstractPostgresType[]
    for i in 1:size(df)[2]
        if issubtype(typeof(df[i]), PooledDataArray)
            val = df[i].pool[1]
        else
            val = df[i].data[1]
        end
        push!(a, PostgresValue(val).pgtype)
    end
    a
end

###########################################################################
### instances

# Oids have to be unique against the _whole_ database,
# while the name of a type only has to be unique per namespace.

# This is everything the manual documents.
# There is more but they are obsolete or are not used for data columns.

#XXX Do not use NaN for naval because (NaN == NaN) == false.
#    this will fail the unit tests for round trips
#    (there is big rant on NaN from the developers.)

base_types = Dict(

    # default if we cannot find it in the Dict
    0            =>  PostgresType{UTF8String}(:jlunknown, UTF8String("∅")),

    16           =>  PostgresType{Bool}(:bool, false),
    17           =>  PostgresType{Vector{UInt8}}(:bytea, UInt8[0]),

    #### BITS
    1560         =>  PostgresType{BitVector}(:bit, BitVector([false])),
    1562         =>  PostgresType{BitVector}(:varbit, BitVector([false])),

    #### NUMBERS
    20           =>  PostgresType{Int64}(:int8, 0),
    21           =>  PostgresType{Int16}(:int2, 0),
    23           =>  PostgresType{Int32}(:int4, 0),
    700          =>  PostgresType{Float32}(:float4, 0),
    701          =>  PostgresType{Float64}(:float8, 0),
    1700         =>  PostgresType{BigFloat}(:numeric, 0),
    # good luck :) depends on locale.
    # Currency package looks pretty sweet for this.
    #790          =>  PostgresType{Float64}(:money, 0),

    #### STRINGS
    # varchar and bpchar (blank padded char) are realy subsets of text.
    # PG does not really have a character type like julia.
    25           =>  PostgresType{UTF8String}(:text, UTF8String("∅")),
    1043         =>  PostgresType{UTF8String}(:varchar, UTF8String("∅")),
    1042         =>  PostgresType{UTF8String}(:bpchar, UTF8String("∅")),
    705          =>  PostgresType{UTF8String}(:unknown, UTF8String("∅")),
    # probably deprecated. Cast it and it always comes out bpchar.
    #18           =>  PostgresType{UTF8String}(:char, UTF8String("∅")),

    #### STRUCTURED TEXT
    # I do not find these necessary to support parsers
    # as text is text so do what you will with it.
    # I know there is already json support in julia
    # but what are you going to do with a DataTable
    # full a json or xml?
    114          =>  PostgresType{UTF8String}(:json, UTF8String("{}")),
    3802         =>  PostgresType{UTF8String}(:jsonb, UTF8String("{}")),
    ### PG not built with xml support on my build
    #142  xml     =>  PostgresType{UTF8String}(:xml, UTF8String("")),

    #### TIME
    1082         =>  PostgresType{Date}(:date, Date()),
    # julia only uses 5 decimals of precision for the seconds :(
    #1114         =>  PostgresType{DateTime}(:timestamp, DateTime()),
    #1083 │ time        
    #1184 │ timestamptz 
    #1266 │ timetz      
    #1186 │ interval
    

    #### OID (INTERNAL) TYPES
    # You never hear of these unless your a Postgres geek.
    #24           =>  PostgresType{Int32}(:regproc, 0),
    #26           =>  PostgresType{Int32}(:oid, 0),
    #2202         =>  PostgresType{Int32}(:regprocedure, 0),
    #2203         =>  PostgresType{Int32}(:regoper, 0),
    #2204         =>  PostgresType{Int32}(:regoperator, 0),
    #2205         =>  PostgresType{Int32}(:regclass, 0),
    #2206         =>  PostgresType{Int32}(:regtype, 0),
    #3734         =>  PostgresType{Int32}(:regconfig, 0),
    #3769         =>  PostgresType{Int32}(:regdictionary, 0),
    #194          =>  PostgresType{UTF8String}(:pg_node_tree, UTF8String("∅")),
    #19           =>  PostgresType{UTF8String}(:name, UTF8String("∅")),
    #3220 │ pg_lsn ;  LSN (Log Sequence Number)

    #### GEOM
    #600 │ point   
    #601 │ lseg    
    #602 │ path    
    #603 │ box     
    #604 │ polygon 
    #628 │ line    
    #718 │ circle  

    #### NETWORK
    #650 │ cidr 
    #869 │ inet 
    #829 │ macaddr; strangely a base user defined type

    #### RANGE
    #3904 │ int4range
    #3906 │ numrange
    #3908 │ tsrange
    #3910 │ tstzrange
    #3912 │ daterange
    #3926 │ int8range

    #MISC.
    #3614 │ tsvector ; text search
    #3615 │ tsquery  ; text serach
    #2950 │ uuid; Universally Unique Identifiers (UUID) 

    # Pseudo-Types are abstract so you should
    # should never see their oid's from libpq fetches
)

base_types_byname = Dict([(t[2].name, t[2]) for t in base_types])



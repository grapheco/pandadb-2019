package org.neo4j.values.storable;

import cn.pandadb.blob.BlobEntry;
import org.neo4j.values.AnyValue;
//import cn.pandadb.blob.Blob;
import org.neo4j.values.ValueMapper;

public class BlobArray extends NonPrimitiveArray<BlobEntry>
{
    BlobEntry[] _blobs;
    BlobArraySupport _support;

    BlobArray( BlobEntry[] blobs )
    {
        this._blobs = blobs;
        _support = new BlobArraySupport( blobs );
    }

    @Override
    public BlobEntry[] value()
    {
        return this._blobs;
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return (T) _blobs;
    }

    @Override
    public String getTypeName()
    {
        return "BlobArray";
    }

    @Override
    public AnyValue value( int offset )
    {
        return _support.values()[offset];
    }

    @Override
    public boolean equals( Value other )
    {
        return _support.internalEquals( other );
    }

    @Override
    int unsafeCompareTo( Value other )
    {
        return _support.unsafeCompareTo( other );
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        _support.writeTo( writer );
    }

    @Override
    public ValueGroup valueGroup()
    {
        return ValueGroup.NO_VALUE;
    }
}

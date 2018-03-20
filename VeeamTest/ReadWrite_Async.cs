using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;

namespace VeeamTest
{
    class ReadWrite_Async
    {

    }


    public class ReadFileThread
    {
        public Thread thread;
        public FileInfo inputFileInfo;
        public int threadsCount;
        public int threadIndex;

        public bool finished = false;


        public ReadFileThread ( FileInfo inputFileInfoToSet, int threadsCountToSet, int threadIndexToSet )
        {
            inputFileInfo = inputFileInfoToSet;
            this.threadsCount = threadsCountToSet;
            this.threadIndex = threadIndexToSet;

            thread = new Thread( DoWork );
            thread.Start();
        }



        void DoWork ()
        {
            using ( var readFileStream = new FileStream( inputFileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.Read ) )
            {
                long index = 0;
                int bytesRead = 0;
                long startReadPosition = 0;
                byte[] buffer;



                //while dataChunkIndexes contains any data => read file at this index
                while ( IsAnyDatachunkNotReaden() )
                {
                    if(( index = IsDataChunkIndexesHaveNotReadenData() ) == -1 ) continue;
                        //Console.Write( " -R " + index );

                    buffer = new byte [ Program.BufferSize ];
                    startReadPosition = Program.dataChunks [ index ].seekIndex;// index * Program.BufferSize;
                    readFileStream.Position = startReadPosition;


                    if ( readFileStream.CanRead &&
                        ( bytesRead = readFileStream.Read( buffer, 0, buffer.Length ) ) > 0 )
                    {
                        if(index==0 
                            || (index>0 
                            && Program.dataChunks[ index - 1 ] != null)
                            && Program.dataChunks [ index - 1 ].currState!=DataChunk.State.waiting 
                            )
                        {
                            //if reach end file and buffer filled with nulls
                            if ( bytesRead < buffer.Length )
                            {
                                buffer = buffer.Take( bytesRead ).ToArray();
                            }


                            var writePos = index > 0 
                                ? Program.dataChunks [ index - 1 ].seekIndex + Program.dataChunks [ index - 1 ].byteData.Length
                                //? Program.dataChunks [ index - 1 ].nextBlockSeekIndex
                                : 0;

                            var newDataChunk = Program.dataChunks[ index ];
                            newDataChunk.currState = DataChunk.State.dataReadingNow;

                            if ( newDataChunk != null )
                            {
                                newDataChunk.byteData = CommonUtils.CompressDataChunk( buffer );
                                newDataChunk.seekIndex = writePos;
                                //newDataChunk.nextBlockSeekIndex = newDataChunk.seekIndex + bytesRead;
                                newDataChunk.currState = DataChunk.State.dataReaded;

                                //Console.Write( " -R " + index );


                                if( Program.readedIndexes.Contains( index ) )
                                {
                                    Console.WriteLine( " -Read duplicate !!! " + index );
                                }
                                else
                                {
                                    Program.readedIndexes.Add( index );
                                }
                            }
                        }
                        



                    }
                }

                finished = true;
            }
        }

        bool IsAnyDatachunkNotReaden ()
        {
            for ( var i = threadIndex; i < Program.dataChunks.Length; i += threadsCount )
            {
                DataChunk dataChunk = Program.dataChunks[ i ];
                if ( dataChunk == null || dataChunk.currState == DataChunk.State.waiting ) return true;
            }
            return false;
        }

        long IsDataChunkIndexesHaveNotReadenData ()
        {
            for ( int i = threadIndex; i < Program.dataChunks.Length; i+=threadsCount )
            {
                var dc = Program.dataChunks[ i ];
                if ( dc != null && dc.byteData == null 
                    //&& dc.currState == DataChunk.State.waiting 
                    )
                {
                    if ( i > 0 )
                    {
                        var dcPrev = Program.dataChunks[ i - 1 ];
                        if ( dcPrev != null 
                            && dcPrev.byteData != null 
                            //&& dcPrev.currState != DataChunk.State.waiting 
                            )
                        {
                            return i;
                        }
                    }
                    else
                    {
                        return i;
                    }
                }
            }

            return -1;
        }
    }

    public class CompressAndWriteFileThread
    {
        public Thread thread;
        public FileInfo outFileInfo;

        public bool finished = false;


        public CompressAndWriteFileThread ( FileInfo outFileInfoToSet )
        {
            outFileInfo = outFileInfoToSet;

            thread = new Thread( DoWork );
            thread.Start();
        }



        void DoWork ()
        {
            if( !File.Exists( outFileInfo.FullName ) )
            {
                var fc = File.Create( outFileInfo.FullName );
                fc.Close();
            }



            using ( FileStream outFileStream = File.OpenWrite( outFileInfo.FullName ) )
            {
                long index = 0;
                int bytesRead = 0;
                long startReadPosition = 0;
                //byte[] buffer;



                //while dataChunkIndexes contains any data => read file at this index
                while ( ( index = IsDataChunkIndexesHaveNotWritenData() ) > -1 )
                {
                    Console.Write( " -W " + index );

                    outFileStream.Position = Program.dataChunks [ index ].seekIndex;

                    using ( GZipStream compressionStream = new GZipStream( outFileStream, CompressionMode.Compress ) )
                    {
                        var dataChunk = Program.dataChunks[ index ];
                        compressionStream.Write( dataChunk.byteData, 0, dataChunk.byteData.Length );
                    }
                }

                finished = true;
            }
        }


        long IsDataChunkIndexesHaveNotWritenData ()
        {
            for ( int i = 0; i < Program.dataChunks.Length; i++ )
            {
                var dc = Program.dataChunks[ i ];
                if ( dc != null && dc.currState == DataChunk.State.dataReaded )
                {
                    if ( i > 0 )
                    {
                        var dcPrev = Program.dataChunks[ i - 1 ];
                        if ( dcPrev != null && dc.currState == DataChunk.State.finished )
                        {
                            return i;
                        }
                    }
                    else
                    {
                        return i;
                    }
                }
            }

            return -1;
        }
    }
}

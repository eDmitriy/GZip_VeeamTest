using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using VeeamTest;


namespace VeeamTest
{
    class Program
    {
        public static int threadCount = 4;
        const long buffSizeMult = 1024;
        private static long BufferSize = buffSizeMult*buffSizeMult;//1024 * 1024;

        public static int threadCounter = 0;
        //static int bytesRead = 1;


        //static List<DataChunk> dataList = new List< DataChunk >();
        static DataChunk[] dataChunks = new DataChunk[0];


        static DateTime startTime;


        static void Main ( string [] args )
        {
            startTime = DateTime.Now;
            if( ChooseAction( ref args ) == 0 )
            {
                Console.WriteLine( string.Format( "Completed in {0}", ( DateTime.Now - startTime ).TotalSeconds ) );
                Console.ReadKey();
            }

        }






        static int ChooseAction( ref string[] args )
        {
            if( args.Length < 2 )
            {
                Console.WriteLine("Wrong parameters, see help/?");
                Console.ReadKey();
                return 1;
            }

            if ( args [ 0 ].Equals( "decompress", StringComparison.InvariantCultureIgnoreCase ) )
            {
                Decompress( args [ 1 ], args [ 2 ] );
            }
            if ( args [ 0 ].Equals( "compress", StringComparison.InvariantCultureIgnoreCase ) )
            {
                //Compress( args [ 1 ], args [ 2 ] );

                Thread thread = new Thread( ReadFileThreaded );
                Thread thread2 = new Thread( WriteCompressedDataTheaded );

                thread.Start(args[1]);


                string outputFileName = args[ 1 ]+".gz";
                if ( args.Length > 2 )
                {
                    outputFileName = args [ 2 ];
                }
                thread2.Start( outputFileName );


                //WriteCompressedDataTheaded( args [ 2 ] );
            }

            return 0;
        }


        static void ReadFileThreaded( object threadData )
        {
            string pathToInputFile = ( string ) threadData;

            using ( var outFileStream = new FileStream( pathToInputFile, FileMode.Open, FileAccess.Read, FileShare.Read ) )
            {
                dataChunks = new DataChunk [(outFileStream.Length/BufferSize)+1];
                int dataChunkIndexesStarted = 0;
                int dataChunkIndexesFinished = 0;

                long readCount = dataChunks.Length;
                List< Thread > threads = new List< Thread >();


                while(dataChunkIndexesFinished < readCount 
                    || ( threads.Count>0 && threads.Count( v => v.IsAlive ) > 0)
                    || threads.Count == 0
                    )
                {
                    if ( dataChunkIndexesStarted < dataChunks.Length && threads.Count( v => v.IsAlive ) < threadCount )
                    {
                        Thread thread = new Thread( (i) =>
                        {
                            var index = ( int ) i;

                            //compressionStream.WriteBlock( bytesRead/threadCount, ref  buffer );
                            Console.Write( " -R "+index);
                            var bytesRead = 0;
                            byte[] buffer = new byte [BufferSize];

                            if( outFileStream.CanRead && (bytesRead = outFileStream.Read( buffer, 0, buffer.Length ))>0)
                            {
                                //if reach end file and buffer filled with nulls
                                if( bytesRead < buffer.Length )
                                {
                                    buffer = buffer.Take( bytesRead ).ToArray();
                                }

                                dataChunks [ index ] = new DataChunk()
                                {
                                    byteData = CommonUtils.CompressDataChunk( buffer ),
                                    seekIndex = bytesRead
                                };
                                dataChunkIndexesFinished++;
                            }

                        } );
                        thread.Start( dataChunkIndexesStarted );
                        threads.Add( thread );

                        dataChunkIndexesStarted++;

                    }


                }
            }
            Console.WriteLine( " Read END" );

        }

        static void WriteCompressedDataTheaded( object threadData )
        {
            string newFileName = ( string ) threadData;

            //if ( ( bytesRead = fStream.Read( buffer, 0, buffer.Length ) ) == 0 ) break; //exit
            while( dataChunks.Length<0 )
            {
                //wait for chunks creation
            }


            using ( FileStream compressedFileStream = File.Create( newFileName) )
            {
                //fStream.CopyTo( compressionStream );
                //List<Thread> threads = new List< Thread >();
                List<long> writedIndexes = new List< long >();

                while( /*bytesRead > 0 ||*/ dataChunks.Length==0 || IsAnyDatachunkNotWriten())
                {
                    for ( var i = 0; i < dataChunks.Length; i++ )
                    {
                        DataChunk dataChunk = dataChunks[ i ];
                        if ( dataChunk != null && dataChunk.currState == DataChunk.State.waiting 
                            && (writedIndexes.Contains( i-1 )|| writedIndexes.Count == 0) 
                            )
                        {
                            Console.Write( " -W "+i );
                            dataChunk.currState = DataChunk.State.working;

                            //compressedFileStream.Seek( dataChunk.seekIndex, SeekOrigin.Begin );

                            compressedFileStream.Write( dataChunk.byteData, 0, /*(int)dataChunk.seekIndex*/dataChunk.byteData.Length );
                            dataChunk.currState = DataChunk.State.finished;

                            writedIndexes.Add( i );

                            //i = 0;
                            break;
                        }
                    }
                }
            }


            /*                FileInfo info = new FileInfo(fileInfo.Directory + "\\" + newFileName/*fileInfo.Name + ".gz"#1#);
                        Console.WriteLine( "\nCompressed {0} from {1} to {2} bytes. \nComprRate = {3} X",
                            fileInfo.Name, fileInfo.Length.ToString(), info.Length.ToString()
                            , ( ( float )fileInfo.Length / ( float )info.Length ) );*/
            Console.WriteLine( " Write END" );
            Console.WriteLine( string.Format( "Completed in {0}", ( DateTime.Now - startTime ).TotalSeconds ) );
            Console.ReadKey();

        }


        static void Compress(string pathToInputFile, string newFileName)
        {
            Console.WriteLine("Compress");

            using ( var fStream = new FileStream( pathToInputFile, FileMode.Open, FileAccess.Read, FileShare.Read ) )
            {
                var fileInfo = new FileInfo( pathToInputFile );


                if ( ( File.GetAttributes( fileInfo.FullName ) &
                        FileAttributes.Hidden ) != FileAttributes.Hidden & fileInfo.Extension != ".gz" )
                {
                    using ( FileStream compressedFileStream = File.Create( newFileName/*fStream.Name + ".gz"*/ ) )
                    {

                        using ( GZipStream compressionStream = new GZipStream( compressedFileStream,
                            CompressionMode.Compress, false ) )
                        {
                            //fStream.CopyTo( compressionStream );
                            List<Thread> threads = new List< Thread >();
                            int bytesRead = 0;
                            byte[] buffer = new byte[0];


                            while ( true/*buffer==null || buffer.Length==0 */ )
                            {
                                //buffer = new byte[BufferSize];
                                if( buffer.Length == 0 ) {
                                    buffer = new byte [ BufferSize ];
                                    if ( (bytesRead = fStream.Read( buffer, 0, buffer.Length ) ) == 0) break; //exit
                                }


                                
  /*                              if ( threads.Count( v => v.IsAlive ) < threadCount )
                                {
                                    Thread thread = new Thread( () =>
                                    {
                                        compressionStream.WriteBlock( bytesRead/threadCount, ref  buffer );
                                    } );
                                    thread.Start();

                                    threads.Add( thread );
                                }*/
                            }


                        }
                    }


                    FileInfo info = new FileInfo(fileInfo.Directory + "\\" + newFileName/*fileInfo.Name + ".gz"*/);
                    Console.WriteLine( "\nCompressed {0} from {1} to {2} bytes. \nComprRate = {3} X",
                        fileInfo.Name, fileInfo.Length.ToString(), info.Length.ToString()
                        ,( (float)fileInfo.Length / (float)info.Length) );
                }

            }
        }





        static void Decompress ( string fileName, string newFileName )
        {
            FileInfo fileToDecompress = new FileInfo( fileName );

            if( fileToDecompress.Extension == ".gz" )
            {
                using ( FileStream originalFileStream = fileToDecompress.OpenRead() )
                {
                    //string currentFileName = fileToDecompress.FullName;
                    //string newFileName = currentFileName.Remove(currentFileName.Length - fileToDecompress.Extension.Length);

                    using ( FileStream decompressedFileStream = File.Create( newFileName ) )
                    {
                        using ( GZipStream decompressionStream =
                            new GZipStream( originalFileStream, CompressionMode.Decompress, false ) )
                        {
                            decompressionStream.CopyTo( decompressedFileStream );
                            Console.WriteLine( "Decompressed: {0}", fileToDecompress.Name );
                        }
                    }
                }
            }
        }




        static bool IsAnyDatachunkNotWriten()
        {
            for ( var i = 0; i < dataChunks.Length; i++ )
            {
                DataChunk dataChunk = dataChunks[ i ];
                if( dataChunk == null || dataChunk.currState == DataChunk.State.waiting ) return true;
            }
            return false;
        }
    }
}


/*public class GZipThread
{
    public Thread thread;

    public GZipStream compressionStream;
    public int read;
    public byte[] buffer;


    public void Start( GZipStream compressionStreamToSet, int readToSet, byte[] bufferToSet )
    {
        this.compressionStream = compressionStreamToSet;
        this.read = readToSet;
        this.buffer = bufferToSet;

        thread = new Thread( DoWork );
        thread.Start();
    }

    void DoWork()
    {
        CommonUtils.WriteBlock( compressionStream, read, ref buffer );
    }
}*/


public class DataChunk
{
    public byte[] byteData;
    public long bytesCount;
    public long seekIndex;

    public enum State
    {
        waiting,
        working,
        finished
    }

    public State currState;
}
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.AccessControl;
using System.Text;
using System.Threading;
using VeeamTest;


namespace VeeamTest
{
    class Program
    {
        public static int threadCount = 4;
        const long buffSizeMult = 1024;
        public static long BufferSize = buffSizeMult*buffSizeMult;//1024 * 1024;



        public static DataChunk[] dataChunks = new DataChunk[0];
        //public static long[] dataChunkIndexes = new long[0];

            
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
            FileInfo inputFileInfo = new FileInfo( pathToInputFile );

            #region InitChunks

            using ( var outFileStream = new FileStream( pathToInputFile, FileMode.Open, FileAccess.Read, FileShare.Read ) )
            {
                dataChunks = new DataChunk [( outFileStream.Length / BufferSize ) + 1];
                for ( int i = 0; i < dataChunks.Length; i++ )
                {
                    dataChunks [ i ] = new DataChunk()
                    {
                        seekIndex = i * BufferSize
                    };
                }
            }

            #endregion

            #region InitThreads-Read

            List<ReadFileThread> gZipThreads = new List< ReadFileThread >();

            for ( int i = 0; i < threadCount; i++ )
            {
                ReadFileThread gZipThread = new ReadFileThread(inputFileInfo);
                //gZipThread.Start( inputFileInfo );

                gZipThreads.Add( gZipThread );
            }

            #endregion

            while( gZipThreads.Any(v=>!v.finished) )
            {
                //wait for threads
            }



            Console.WriteLine( "" );
            Console.WriteLine( " Read END" );
        }

        static void WriteCompressedDataTheaded( object threadData )
        {
            string newFileName = ( string ) threadData;

            while( dataChunks.Length<0 )
            {
                //wait for chunks creation
            }


            using ( FileStream outFileStream = File.Create( newFileName, (int)buffSizeMult, FileOptions.Asynchronous ) )
            {
                using ( GZipStream compressionStream = new GZipStream( outFileStream, CompressionMode.Compress ) )
                {
                    
                    List< long > writedIndexes = new List< long >();

                    while( dataChunks.Length == 0 || IsAnyDatachunkNotWriten() )
                    {
                        for ( var i = 0; i < dataChunks.Length; i++ )
                        {
                            DataChunk dataChunk = dataChunks[ i ];
                            if( dataChunk != null && dataChunk.currState == DataChunk.State.dataReaded
                                && (writedIndexes.Contains( i-1 )|| writedIndexes.Count == 0) 
                            )
                             {
                                Console.Write( " -W "+i );
                                dataChunk.currState = DataChunk.State.dataWritingNow;

                                /*outFileStream*/
                                 //compressionStream.Position = dataChunk.seekIndex;

                                /*outFileStream*/
                                 compressionStream.Write( dataChunk.byteData, 0, dataChunk.byteData.Length );
                                dataChunk.currState = DataChunk.State.finished;

                                writedIndexes.Add( i );

                                break;
                             }
                        }
                    }
                }
            }


            /*                FileInfo info = new FileInfo(fileInfo.Directory + "\\" + newFileName/*fileInfo.Name + ".gz"#1#);
                        Console.WriteLine( "\nCompressed {0} from {1} to {2} bytes. \nComprRate = {3} X",
                            fileInfo.Name, fileInfo.Length.ToString(), info.Length.ToString()
                            , ( ( float )fileInfo.Length / ( float )info.Length ) );*/
            Console.WriteLine( "" );
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
                if( /*dataChunk == null || */dataChunk.currState != DataChunk.State.finished ) return true;
            }
            return false;
        }
    }
}


public class ReadFileThread
{
    public Thread thread;
    public FileInfo inputFileInfo;

    public bool finished = false;


    public ReadFileThread ( FileInfo outFileInfoToSet )
    {
        inputFileInfo = outFileInfoToSet;

        thread = new Thread( DoWork );
        thread.Start();
    }



    void DoWork()
    {
        using ( var readFileStream = new FileStream( inputFileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.Read ) )
        {
            long index = 0;
            int bytesRead = 0;
            long startReadPosition = 0;
            byte[] buffer;



            //while dataChunkIndexes contains any data => read file at this index
            while( (index = IsDataChunkIndexesHaveNotReadenData()) > -1 )
            {
                Console.Write( " -R " + index );

                buffer = new byte [ Program.BufferSize ];
                startReadPosition = Program.dataChunks[ index ].seekIndex;// index * Program.BufferSize;
                readFileStream.Position = startReadPosition;


                if( readFileStream.CanRead &&
                    ( bytesRead = readFileStream.Read( buffer, 0, buffer.Length ) ) > 0 )
                {
                    //if reach end file and buffer filled with nulls
                    if( bytesRead < buffer.Length )
                    {
                        buffer = buffer.Take( bytesRead ).ToArray();
                    }


                    //stream write position index
                    var writePos = index > 0 && Program.dataChunks [ index - 1 ]!=null 
                        ? Program.dataChunks [ index - 1 ].seekIndex + Program.dataChunks [ index - 1 ].byteData.Length
                        : 0;


                    var newDataChunk = Program.dataChunks[ index ];
                    if( newDataChunk != null )
                    {
                        newDataChunk.byteData = CommonUtils.CompressDataChunk( buffer );
                        newDataChunk.seekIndex = writePos;
                        newDataChunk.currState = DataChunk.State.dataReaded;
                    }
                }
            }

            finished = true;
        }
    }


    long IsDataChunkIndexesHaveNotReadenData()
    {
        for ( int i = 0; i < Program.dataChunks.Length; i++ )
        {
            var dc = Program.dataChunks[ i ];
            if( dc != null && dc.byteData == null )
            {
                if( i > 0 )
                {
                    var dcPrev = Program.dataChunks[ i - 1 ];
                    if( dcPrev != null && dcPrev.byteData != null )
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


public class DataChunk
{
    public byte[] byteData;
    public long seekIndex = -1;

    public enum State
    {
        waiting,
        dataReaded,
        dataWritingNow,
        finished
    }

    public State currState;
}
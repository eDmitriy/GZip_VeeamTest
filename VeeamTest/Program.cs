using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;



namespace VeeamTest
{
    class Program
    {
        #region Vars

        public static int threadCount = 20;
        const long buffSizeMult = 1024;
        public static long BufferSize = buffSizeMult*buffSizeMult;//1024 * 1024;

        public static DataBlock[] dataBlocks = new DataBlock[0];
/*        public static Queue<DataBlock> queue_Read = new Queue< DataBlock >();
        public static Queue<DataBlock> queue_Write = new Queue< DataBlock >();*/

        //public static List<long> readedIndexes = new List< long >();
        //public static List<long> writedIndexes = new List< long >();
        //public static int writeBlockTotalCount = 0;

        public static Queue<long> comprssedFileHeadersQueue = new Queue< long >();
        public static List<long> compressedBlocksheadersIndexList = new List< long >();
        public static int headersFound = 0;

        static DateTime startTime;

        #endregion




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
                var writeBlockTotalCount = ( int )( outFileStream.Length / BufferSize ) + 1;
                dataBlocks = new DataBlock [ writeBlockTotalCount ];
                for ( int i = 0; i < dataBlocks.Length; i++ )
                {
                    dataBlocks [ i ] = new DataBlock();
                }
            }

            #endregion

            #region InitThreads-Read

            List<CompressionThread> gZipThreads = new List< CompressionThread >();

            for ( int i = 0; i < threadCount; i++ )
            {
                CompressionThread gZipThread = new CompressionThread(
                    inputFileInfo, 
                    //i * (dataChunks.Length/threadCount) 
                    threadCount, i
                    );
                gZipThreads.Add( gZipThread );
            }

            #endregion

            while( gZipThreads.Any(v=>!v.Finished) )
            {
                //wait for threads
            }
            gZipThreads.Clear();
            GC.Collect();


            Console.WriteLine( "" );
            Console.WriteLine( " Read END" );
        }

        static void WriteCompressedDataTheaded( object threadData )
        {
            string newFileName = ( string ) threadData;
            FileInfo outFileInfo = new FileInfo( newFileName );

            while ( dataBlocks==null || dataBlocks.Length<1 )
            {
                //wait for chunks creation
            }


            if( File.Exists( outFileInfo.FullName ) )
            {
                File.Delete( outFileInfo.FullName );
            }

            //using ( FileStream outFileStream = File.Create( outFileInfo.FullName ) )
            using ( FileStream outFileStream = new FileStream( outFileInfo.FullName, FileMode.Append, FileAccess.Write ) )
            {
                outFileStream.Lock( 0, dataBlocks.Length*BufferSize );

                long index = 0;
                int bytesRead = 0;
                long startReadPosition = 0;


                //for every block do write
                for ( int i = 0; i < /*writeBlockTotalCount*/dataBlocks.Length; i++ )
                {
                    index = i;

                    while( dataBlocks [ i ]==null || dataBlocks [i].byteData==null ){/* wait for chunkData */}



                    DataBlock dataChunk = dataBlocks[ index ];
                    dataChunk.currState = DataBlock.State.dataWritingNow;

                    //BitConverter.GetBytes( dataChunk.byteData.Length ).CopyTo( dataChunk.byteData, 4 );
                    outFileStream.Write( dataChunk.byteData, 0, dataChunk.byteData.Length );


                    dataChunk.currState = DataBlock.State.finished;
                    dataChunk.byteData=new byte[0];
                    Console.Write( " -W " + index );
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






        static void Decompress ( string fileName, string newFileName )
        {
            FileInfo fileToDecompress = new FileInfo( fileName );

            if( fileToDecompress.Extension == ".gz" )
            {
                using ( FileStream originalFileStream = fileToDecompress.OpenRead() )
                {
                    if ( File.Exists( newFileName ) )
                    {
                        File.Delete( newFileName );
                    }


                    using ( FileStream decompressedFileStream = new FileStream( newFileName, FileMode.Append, FileAccess.Write ) )
                    {
                        long streamPos = 0;
                        long prevBytesRead = -1;
                        var bytesRead = 0;
                        byte[] buffer = new byte[BufferSize];


                        List< ReadCompressedFileHeadersThread > readThreadsList = new List< ReadCompressedFileHeadersThread >();



                        //Start threaded headers info reading of compressed blocks in file 
                        for ( int i = 0; i < threadCount; i++ )
                        {
                            ReadCompressedFileHeadersThread readCompressedFileHeaders =
                                new ReadCompressedFileHeadersThread(fileToDecompress, threadCount, i );
                            readThreadsList.Add( readCompressedFileHeaders );
                        }



                        //decompress
                        while ( readThreadsList.Any(v=> v.Finished==false) )
                        {
                            if( compressedBlocksheadersIndexList.Count==0 ) continue;
                            //if(comprssedFileHeadersQueue.Count==0) continue;

                            var newPos = compressedBlocksheadersIndexList[ 0 ];
                            compressedBlocksheadersIndexList.RemoveAt( 0 );
                            //var newPos = comprssedFileHeadersQueue.Dequeue();
                            originalFileStream.Position = newPos;


                            using ( GZipStream decompressionStream =
                                new GZipStream( originalFileStream, CompressionMode.Decompress, true ) )
                            {
                                while ( ( bytesRead = decompressionStream.Read( buffer, 0, buffer.Length ) ) > 0 )
                                {
                                    decompressedFileStream.Write( buffer, 0, bytesRead );
                                }

                                Console.WriteLine( "decStr pos = " + originalFileStream.Position 
                                    + " " + decompressedFileStream .Position 
                                    + " " + decompressedFileStream.Length);
                            }
                        }
                    }
                    Console.WriteLine( "Decompressed: {0}", fileToDecompress.Name );
                }
            }
        }


    }
}


public class DataBlock
{
    public byte[] byteData;
    //public long seekIndex = -1;

    public enum State
    {
        waiting,
        dataReadingNow,
        dataReaded,
        dataWritingNow,
        finished
    }

    public State currState;
}
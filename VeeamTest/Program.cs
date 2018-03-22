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

        public static int threadCount = Environment.ProcessorCount*5; //magic number 5 !!! because 20 threads work faster than 4
        public static long maxUsedMemory = 1024 * 1024 * 20;
        public static long BufferSize = 1024*1024;

        public static DataBlock[] dataBlocks = new DataBlock[0];

        static ReadCompressedFileHeadersThread[] readCompressed = new ReadCompressedFileHeadersThread[0];
        public static int headersFound = 0;

        static DateTime startTime;

        #endregion




        static void Main ( string [] args )
        {
            int nBufferWidth = Console.BufferWidth;
            int nBufferHeight = 501;
            Console.SetBufferSize( nBufferWidth, nBufferHeight );


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
                //ReadCompressed( args[ 1 ] );
                string[] strings = args;
                Thread thread = new Thread(()=> ReadCompressed(strings [ 1 ]));
                thread.IsBackground = true;
                thread.Start( );

                Decompress( args [ 1 ], args [ 2 ] );
            }
            if ( args [ 0 ].Equals( "compress", StringComparison.InvariantCultureIgnoreCase ) )
            {
                //Compress( args [ 1 ], args [ 2 ] );

                Thread thread = new Thread( ReadFileThreaded );
                thread.IsBackground = true;

                Thread thread2 = new Thread( WriteCompressedDataTheaded );
                thread2.IsBackground = true;

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


            using ( FileStream outFileStream = File.Create( outFileInfo.FullName ) )
            {
                outFileStream.Lock( 0, dataBlocks.Length*BufferSize );

                long index = 0;

                //for every block do write
                for ( int i = 0; i < dataBlocks.Length; i++ )
                {
                    index = i;

                    while( dataBlocks [i].byteData==null ){/* wait for chunkData */}
                    
                    DataBlock dataChunk = dataBlocks[ index ];
                    outFileStream.Write( dataChunk.byteData, 0, dataChunk.byteData.Length );
                    
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





        static void ReadCompressed( string fileName )
        {
            FileInfo fileToDecompress = new FileInfo( fileName );
            if ( fileToDecompress.Extension != ".gz" ) return;

            long originalFileSize = fileToDecompress.Length;
            int threadsIndex = 0;
            maxUsedMemory = Math.Min( maxUsedMemory, originalFileSize );
            //if( maxUsedMemory <originalFileSize) maxUsedMemory

            //Start threaded headers info reading of compressed blocks in file 
            long blockCount = (originalFileSize / ( maxUsedMemory / threadCount ));
            readCompressed = new ReadCompressedFileHeadersThread[blockCount];
            ReadCompressedFileHeadersThread[] activeReadThreads = new ReadCompressedFileHeadersThread [threadCount];

/*            for ( int i = 0; i < /*threadCount#1#blockCount; i+=threadCount )
            {

            }*/
            while( readCompressed[readCompressed.Length-1]==null )
            {
                for ( int j = 0; j < threadCount; j++ )
                {
                    if ( activeReadThreads [ j ] == null 
                        ||( activeReadThreads [ j ].Finished && activeReadThreads [ j ].queueManager.GetQueueCount()==0 ) )
                    {
                        ReadCompressedFileHeadersThread rWorker =
                            new ReadCompressedFileHeadersThread(fileToDecompress, /*threadCount*/(int)blockCount, threadsIndex );
                        if( readCompressed.Length > threadsIndex ) readCompressed [ threadsIndex ] = rWorker;
                        threadsIndex++;
                        
                        activeReadThreads [ j ] = rWorker;
                    }
                }
            }
        }


        static void Decompress ( string fileName, string newFileName )
        {
            FileInfo fileToDecompress = new FileInfo( fileName );
            if( fileToDecompress.Extension != ".gz" ) return;

            //long originalFileSize = fileToDecompress.Length;
            while( readCompressed == null || readCompressed.Length == 0 )
            {
                //Console.WriteLine("Wait for read threads starts");
            }

            using ( FileStream decompressedFileStream = File.Create( newFileName ) )
            {
                int threadsIndex = 0;
                int writeCounter = 1;


                //decompress
                long length = readCompressed.Length-1;

                while ( readCompressed [ length ] == null 
                    || !readCompressed [ length ].Finished 
                    || readCompressed [ length ].queueManager.GetQueueCount() > 0 
                    /*readCompressed.Any(v=>v==null || ( v.Finished==false || v.dataBlocksQueue.Count>0)) */)
                {
                    //long newPos = 0;
                    var thread = readCompressed[ threadsIndex ];
                    if(thread==null) continue;
                    if( thread.Finished && thread.queueManager.GetQueueCount() == 0)
                    {
                        threadsIndex++;
                        continue;
                    }
                    if( thread.queueManager.GetQueueCount() == 0) continue;


                    var newDataBlock = thread.queueManager.Dequeue();
                    decompressedFileStream.Write( newDataBlock.byteData, 0, newDataBlock.byteData.Length );

                    Console.WriteLine( /*"decStr pos = " + originalFileStream.Position
                                        + */" counter = " + writeCounter++ );
                }
            }
            Console.WriteLine( "Decompressed: {0}", fileToDecompress.Name );
            GC.Collect();

        }


    }
}


public class DataBlock 
{
    public byte[] byteData;
}
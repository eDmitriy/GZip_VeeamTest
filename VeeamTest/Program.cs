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

        private int threadCount = Environment.ProcessorCount;
        private readonly long BufferSize = 1024*1024;

        private DateTime startTime;
        private string currOperation = "";

        private Writer outFileWriter = null;

        #endregion



        static void Main ( string [] args )
        {
            var program = new Program();
            program.DoWork( args );
        }




        public void DoWork( string [] args )
        {
            Console.CancelKeyPress += new ConsoleCancelEventHandler( CancelKeyPress );
            ShowInfo();

            try
            {
                #region Validation

                Validation.HardwareValidation();
                Validation.StringReadValidation( args );

                #endregion

                startTime = DateTime.Now;
                CreateWorkers( args );
            }

            catch ( Exception ex )
            {
                Console.WriteLine( "Error is occured!\n Method: {0}\n Error description: {1}", ex.TargetSite, ex.Message );
                //return 1;
            }


            #region Result

            Console.WriteLine( string.Format( "\nCompleted in {0} seconds", ( DateTime.Now - startTime ).TotalSeconds ) );
            Console.WriteLine( "\nHeaders found = " + DecompressReader.headersFound.Count );
            Console.WriteLine( "Writes count = " + Writer.writedIndexes.Count );

            #endregion

            Console.WriteLine( "\nPress any key to exit!" );
            Console.ReadKey();
        }

        
        int CreateWorkers( string[] args )
        {
            currOperation = args[ 0 ].ToLower();
            Console.WriteLine( "\n\n"+currOperation + "ion of " + args [ 1 ] + " started..." );


            #region Create Reader

            Func< ThreadedReader.ThreadedReaderParameters, ThreadedReader > func = null;

            #region Select reader func

            if ( currOperation.Equals( "decompress" ) )
            {
                func = CreateDecompressReader;
            }
            if ( currOperation.Equals( "compress" ) )
            {
                func = CreateCompressReader;
            }

            #endregion

            Thread thread_Read = null;
            thread_Read = new Thread( () => StartReaders( args [ 1 ], func ) );
            thread_Read.IsBackground = true;

            thread_Read.Start();

            #endregion


            #region Create Writer

            outFileWriter = new Writer( args[ 2 ] );
            outFileWriter.RunWorkerAsync();

            #endregion


            //wait for read and write ends
            if ( thread_Read != null)
            {
                while( /*ThreadedReader.registeredReaders.Any(v=>!v.Finished)*/ thread_Read.IsAlive || outFileWriter.GetQueueCount()>0 )
                {
                    Thread.Sleep( 10 );
                }
            }
            if( outFileWriter .IsBusy) outFileWriter.CancelAsync();

            return 0;
        }




        void StartReaders( object threadData, Func<ThreadedReader.ThreadedReaderParameters, ThreadedReader> func)
        {
            string pathToInputFile = ( string ) threadData;
            FileInfo inputFileInfo = new FileInfo( pathToInputFile );
            List<ThreadedReader> gZipThreads = new List< ThreadedReader >();


            // Divide input file to blocks
            ulong writeBlockTotalCount = ( ulong )( inputFileInfo.Length / BufferSize ) + 1;
            
            // Read input file by blocks with threads
            for ( int i = 0; i < threadCount; i++ )
            {
                if( func !=null)
                {
                    var newFuncParams = new ThreadedReader.ThreadedReaderParameters()
                    {
                        inputFileInfo = inputFileInfo,
                        threadsCount = threadCount,
                        threadIndex = i,
                        iterationsTotalCount = writeBlockTotalCount,
                        writer = outFileWriter,
                        bufferSize = BufferSize
                    };
                    ThreadedReader reader = func.Invoke( newFuncParams );
                    reader.RunWorkerAsync();
                    gZipThreads.Add( reader );
                }
            }


            //wait for threads
            while ( gZipThreads.Any(v=>!v.Finished) )
            {
                Thread.Sleep( 100 );
            }

            gZipThreads.Clear();
            GC.Collect();


/*            Console.WriteLine( "" );
            Console.WriteLine( " Read END" );*/
        }

        #region Reader funcs

        CompressReader CreateCompressReader ( ThreadedReader.ThreadedReaderParameters parameters )
        {
            return new CompressReader( parameters );
        }
        DecompressReader CreateDecompressReader ( ThreadedReader.ThreadedReaderParameters parameters )
        {
            return new DecompressReader( parameters );
        }

        #endregion


        #region Utils

        static void ShowInfo ()
        {
            Console.WriteLine( "To zip or unzip files please proceed with the following pattern to type in:\n" +
                               "Zipping: GZipTest.exe compress [Source file path] [Destination file path]\n" +
                               "Unzipping: GZipTest.exe decompress [Compressed file path] [Destination file path]\n" +
                               "To complete the program correct please use the combination CTRL + C" );
        }


        static void CancelKeyPress ( object sender, ConsoleCancelEventArgs _args )
        {
            if ( _args.SpecialKey == ConsoleSpecialKey.ControlC )
            {
                Console.WriteLine( "\nCancelling..." );
                _args.Cancel = true;
                Environment.Exit( 1 );
            }
        }


        public static void ClearCurrentConsoleLine ()
        {
            int currentLineCursor = Console.CursorTop;
            Console.SetCursorPosition( 0, Console.CursorTop );
            Console.Write( new string( ' ', Console.WindowWidth ) );
            Console.SetCursorPosition( 0, currentLineCursor );
        }

        #endregion

    }
}
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
        private const ulong maxMemoryPerThread = 1024 * 1024 * 100; //100mb

        private DateTime startTime;
        private string currOperation = "";


        private Writer writer = null;
        private List<ThreadedReader> readers = new List< ThreadedReader >();

        private ManualResetEvent[] readerDoneEvents = new ManualResetEvent[0];
        private ManualResetEvent writerDoneEvent = new ManualResetEvent(false);

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

            #region CreateReaderEvents

            readerDoneEvents = new ManualResetEvent [ threadCount ];
            for ( int i = 0; i < threadCount; i++ )
            {
                readerDoneEvents[i] = new ManualResetEvent( false );
            }

            #endregion
            
            try
            {
                // Validation
                Validation.HardwareValidation((ulong)threadCount, maxMemoryPerThread );
                Validation.StringReadValidation( args );

                startTime = DateTime.Now;

                CreateWorkers( args );
                WaitForProcessDone();
            }

            catch ( Exception ex )
            {
                Console.WriteLine( "Error is occured!\n Method: {0}\n Error description: {1}", ex.TargetSite, ex.Message );
                return;
            }

            ShowSuccesResult();
            GC.Collect();
        }



        int CreateWorkers( string[] args )
        {
            currOperation = args[ 0 ].ToLower();
            Console.WriteLine( "\n\n"+currOperation + "ion of " + args [ 1 ] + " started..." );
            
            //create writer
            writer = new Writer( args [ 2 ], writerDoneEvent );
            writer.RunWorkerAsync();

            //create reader
            SelectReaderType( args );

            return 0;
        }


        void SelectReaderType(string[] args)
        {
            Func< ThreadedReader.ThreadedReaderParameters, ThreadedReader > func = null;

            if ( currOperation.Equals( "decompress" ) )
            {
                func = CreateDecompressReader;
            }
            if ( currOperation.Equals( "compress" ) )
            {
                func = CreateCompressReader;
            }
            
            StartReaders( args[ 1 ], func );
        }

        void StartReaders( string pathToInputFile, Func<ThreadedReader.ThreadedReaderParameters, ThreadedReader> func)
        {
            FileInfo inputFileInfo = new FileInfo( pathToInputFile );

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
                        writer = writer,
                        bufferSize = BufferSize,
                        doneEvent = readerDoneEvents[i]
                    };
                    ThreadedReader reader = func.Invoke( newFuncParams );
                    readers.Add( reader );
                    reader.RunWorkerAsync();
                }
            }
            
            //wait for threads
/*            WaitHandle.WaitAll( readerDoneEvents );

            readers.Clear();
            GC.Collect();*/

            //Console.WriteLine( "\n Read END" );
        }



        void WaitForProcessDone()
        {
            //wait for read and write queue ends
            WaitHandle.WaitAll( readerDoneEvents );
            writerDoneEvent.WaitOne();

            //cancel writer process if still exists
            //if ( outFileWriter.IsBusy ) outFileWriter.CancelAsync();
        }

        void ShowSuccesResult()
        {
            Console.WriteLine( string.Format( "\nCompleted in {0} seconds", ( DateTime.Now - startTime ).TotalSeconds ) );
            //Console.WriteLine( "\nHeaders found = " + DecompressReader.headersFound.Count );
            Console.WriteLine( "Writes count = " + Writer.writedIndexes.Count );
            
            Console.WriteLine( "\nPress any key to exit!" );
            Console.ReadKey();
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

        void ShowInfo ()
        {
            Console.WriteLine( "To zip or unzip files please proceed with the following pattern to type in:\n" +
                               "Zipping: GZipTest.exe compress [Source file path] [Destination file path]\n" +
                               "Unzipping: GZipTest.exe decompress [Compressed file path] [Destination file path]\n" +
                               "To complete the program correct please use the combination CTRL + C" );
        }


        void CancelKeyPress ( object sender, ConsoleCancelEventArgs _args )
        {
            if ( _args.SpecialKey == ConsoleSpecialKey.ControlC )
            {
                Console.WriteLine( "\nCancelling..." );
                _args.Cancel = true;

                //cancell readers
                foreach ( var r in readers )
                {
                    if(r.IsBusy) r.CancelAsync();
                }

                //cancell writer
                if( writer.IsBusy) writer.CancelAsync();

                //close application
                Environment.Exit( 1 );
            }
        }


        #endregion

    }
}
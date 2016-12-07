﻿using System;
using System.Diagnostics;
using System.ServiceProcess;
using System.IO;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.File;
using System.Configuration;
using System.Management;
using System.Timers;

namespace JBLFileWatchService
{
    public partial class JBLFileWatchService : ServiceBase
    {
        // filewatch variables
        static FileSystemWatcher watcher = new FileSystemWatcher();
        static string cloudShareName = "jblstorage";
        static string cloudTargetDirName = "pstemp";
        static string localWatchDirName = @"J:\";
        static string localTempFolder = @"J:\Temp\";
        static CloudFileDirectory targetdir;
        static DateTime lastRead = DateTime.MinValue;
        static string lastFileChecked = "";

        //azcopy backup process variables
        static int maintHr = -1;
        static DateTime nextAZCopyDate = DateTime.MinValue; 
        static System.Timers.Timer azcopyTimer;
        static string azCopyCommand = "";
        static string azCopyArgument = "";

        public JBLFileWatchService()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {

            if (ConfigurationManager.AppSettings["AttachDebugger"] == "1")
            {
                // allow time to attach debugger
                System.Threading.Thread.Sleep(45000);
            }
            
            cloudShareName = ConfigurationManager.AppSettings["FileShare"];
            cloudTargetDirName = ConfigurationManager.AppSettings["TargetFolder"];
            localWatchDirName = ConfigurationManager.AppSettings["WatchFolder"];
            localTempFolder = ConfigurationManager.AppSettings["WatchFolderTemp"];

            if (localTempFolder != "")
            {
                if (Directory.Exists(localTempFolder) == false)
                {
                    try
                    {
                        Directory.CreateDirectory(localTempFolder);
                    } catch (Exception e)
                    {
                        EventLog.WriteEntry("JBLFileWatchService", "Error creating temp folder [" + localTempFolder + "]:" + e.Message , EventLogEntryType.Error );
                        localTempFolder = "";
                    }
                }
            }
            

            // Create a new FileSystemWatcher and set its properties.
            watcher.Path = localWatchDirName; //args[1];
            /* Watch for changes in LastAccess and LastWrite times, and
               the renaming of files or directories. */
            watcher.NotifyFilter = NotifyFilters.LastAccess | NotifyFilters.LastWrite
               | NotifyFilters.FileName;
            // Only watch pdf files.
            watcher.Filter = ConfigurationManager.AppSettings["FileMask"]; // should be *.pdf

            // Add event handlers.
            watcher.Changed += new FileSystemEventHandler(OnChanged);
            watcher.Created += new FileSystemEventHandler(OnChanged);
            watcher.Deleted += new FileSystemEventHandler(OnChanged);
            watcher.Renamed += new RenamedEventHandler(OnRenamed);

            Microsoft.WindowsAzure.Storage.Auth.StorageCredentials cred = new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(ConfigurationManager.AppSettings["AzureAccount"], ConfigurationManager.AppSettings["AzureKey"]);

            CloudStorageAccount storageAccount = new CloudStorageAccount(cred, false);

            // Create a file client for interacting with the file service.
            CloudFileClient fileClient = storageAccount.CreateCloudFileClient();
            CloudFileShare share = fileClient.GetShareReference(cloudShareName);
            CloudFileDirectory root = share.GetRootDirectoryReference();

            targetdir = root.GetDirectoryReference(cloudTargetDirName);

            // setup a process to run azcopy once a day to miss any events?
            byte b;
            if ((byte.TryParse(ConfigurationManager.AppSettings["AZCopyHr"], out b)) && (Convert.ToInt32(ConfigurationManager.AppSettings["AZCopyHr"]) < 24))
            {
                // between 0 and 23
                maintHr = Convert.ToInt32(ConfigurationManager.AppSettings["AZCopyHr"]);
                azCopyCommand = ConfigurationManager.AppSettings["AzCopyCommand"];
                azCopyArgument = ConfigurationManager.AppSettings["AzCopyArgument"];
            }
            if ((maintHr >=0) && (azCopyCommand !="") && (azCopyArgument !="")) 
            {
                // we have all of the params (note - didn't validate azCopyCommand or Arg. but will assume that if they are set, they are correct
                // Create a timer with a 1 min. interval.
                azcopyTimer = new System.Timers.Timer(60000);

                // Hook up the Elapsed event for the timer.
                azcopyTimer.Elapsed += new ElapsedEventHandler(OnTimedEvent);

                //activate
                azcopyTimer.Enabled = true;

                EventLog.WriteEntry("JBLFileWatchService", "RunAndWait Status: " + maintHr.ToString() + " cmd:" + azCopyCommand + " arg:" + azCopyArgument + " "  + azcopyTimer.Enabled.ToString() , EventLogEntryType.Information);

            }

            // Begin watching.
            watcher.EnableRaisingEvents = true;
        }

        private static void OnTimedEvent(object source, ElapsedEventArgs e)
        {
            if (ConfigurationManager.AppSettings["LogTimerEvents"] == "1" )
            {
                EventLog.WriteEntry("JBLFileWatchService", "TimedEvent: " + nextAZCopyDate.ToString() + " "  + DateTime.Now.Hour.ToString() + " " + maintHr.ToString() , EventLogEntryType.Information);

            }

            // last run > 23 ago, run and then update last run value 
            if ((DateTime.Now >= nextAZCopyDate) && (DateTime.Now.Hour == maintHr))
            {
                azcopyTimer.Enabled = false; //pause timer

                // if the current time is >= to the next time to run,
                // and this hour is the hour to run,
                // increase the next time to run by 24 hrs 
                nextAZCopyDate = DateTime.Now.AddDays(1);

                // suspend watching events during AzCopy
                watcher.EnableRaisingEvents = false;

                if (ConfigurationManager.AppSettings["LogTimerEvents"] == "1")
                {
                    EventLog.WriteEntry("JBLFileWatchService", "RunAndWait Debug: " + watcher.EnableRaisingEvents.ToString() + " " + nextAZCopyDate.ToString() + " " + DateTime.Now.Hour.ToString() + " " + maintHr.ToString() + " cmd:" + azCopyCommand + " arg:" + azCopyArgument, EventLogEntryType.Information);
                }

                try
                {
                    int ret;
                    ret = runAndWait(azCopyCommand, azCopyArgument, 60000 * 12, ""); //should run really fast but allow 12 min?
                }
                catch(Exception ex)
                {
                    EventLog.WriteEntry("JBLFileWatchService", "RunAndWait Exception: " + ex.Message + " cmd:" + azCopyCommand + " arg:" + azCopyArgument , EventLogEntryType.Error );  
                }
                finally
                {
                    //always make sure events are enabled
                    watcher.EnableRaisingEvents = true;
                }
            }
            //always make sure events are enabled
            watcher.EnableRaisingEvents = true;
            azcopyTimer.Enabled = true;
            //EventLog.WriteEntry("JBLFileWatchService", "RunAndWait Debug Post: " + watcher.EnableRaisingEvents.ToString() + " " + nextAZCopyDate.ToString() + " " + DateTime.Now.Hour.ToString()  + " " + maintHr.ToString()  + " cmd:" + azCopyCommand + " arg:" + azCopyArgument, EventLogEntryType.Error);
        }

        protected override void OnStop()
        {
            
            if (watcher.EnableRaisingEvents == false)
            {
                //should pause here because runAndWait is running
                System.Threading.Thread.Sleep(30000);
            }
        }

        private static void OnChanged(object source, FileSystemEventArgs e)
        {
            DateTime lastWriteTime = File.GetLastWriteTime(e.FullPath);

            ////.tmp PDF files seem to be detected; manually check filenames
            //if (watcher.Filter.Substring(watcher.Filter.Length - 3).ToLower() == e.Name.Substring(e.Name.Length -3).ToLower())
            //{
            if ((lastWriteTime != lastRead) | (lastFileChecked != e.Name))
            {
                lastRead = lastWriteTime;
                lastFileChecked = e.Name;
                //EventLog.WriteEntry("JBLFileWatchService", "File: " + e.Name + " " + e.ChangeType, EventLogEntryType.Information);  
                if (e.ChangeType == WatcherChangeTypes.Deleted)
                {
                    azureDelete(e.Name);
                }
                else
                {
                    azureUpload(e.Name, e.FullPath, true);
                }
            }
            //}  


        }

        private static void OnRenamed(object source, RenamedEventArgs e)
        {
            //EventLog.WriteEntry("JBLFileWatchService", "File: " + e.OldName + " " + e.ChangeType + " " + e.Name, EventLogEntryType.Information);
            azureRename(e.OldName, e.Name, e.FullPath);
        }

        //static async Task azureUploadAsync(string Filename, string fullPath)
        //{
        //    CloudFile file = targetdir.GetFileReference(Filename);
        //    await file.UploadFromFileAsync(fullPath); //upload (overwrite if exists) new file

        //}
        //static async Task azureRename(string OldFilename, string NewFilename, string NewFullPath)
        //{
        //    await azureUploadAsync(NewFilename, NewFullPath); // upload new
        //    if (OldFilename.ToLower() != NewFilename.ToLower())
        //    {
        //        azureDelete(OldFilename); // delete old
        //    }

        //}
        static void azureUpload(string Filename, string fullPath, bool direct)
        {
            try
            {
                CloudFile file = targetdir.GetFileReference(Filename);
                file.UploadFromFile(fullPath); //upload (overwrite if exists) new file
            }
            catch(Exception e)
            {
                EventLog.WriteEntry("JBLFileWatchService", "azureUpload " + fullPath + " "  + e.Message , EventLogEntryType.Error);
                if ((localTempFolder != "") && (direct == true ))
                {
                    try
                    {
                        // try copying file to temp folder and then upload it from temp
                        File.Copy(fullPath, localTempFolder + Filename, true);
                        azureUpload(Filename, localTempFolder + Filename, false);
                        EventLog.WriteEntry("JBLFileWatchService", "azureUpload re-trying from " + localTempFolder + Filename, EventLogEntryType.Information );
                        // delete temp version
                        File.Delete(localTempFolder + Filename);
                    } catch (Exception e1)
                    {
                        EventLog.WriteEntry("JBLFileWatchService", "azureUpload re-try failure from " + localTempFolder + Filename + " " + e1.Message , EventLogEntryType.Error );
                    }
                }
            } 
            

        }
        static void azureRename(string OldFilename, string NewFilename, string NewFullPath)
        {
            string debug = "";
            try
            {
                debug = "New: " + NewFilename;
                azureUpload(NewFilename, NewFullPath, true); // upload new
                if (OldFilename.ToLower() != NewFilename.ToLower())
                {
                    debug = "Del: " + OldFilename;
                    azureDelete(OldFilename); // delete old
                }
            }
            catch (Exception e)
            {
                EventLog.WriteEntry("JBLFileWatchService", "azureRename " + NewFullPath + "Debug: " + debug + " " + e.Message, EventLogEntryType.Error);
            }
            
        }
        static void azureDelete(string OldFilename)
        {
            // delete file in cloud
            try
            {
                CloudFile file = targetdir.GetFileReference(OldFilename);
                bool ret = file.DeleteIfExists();
            }
            catch (Exception e)
            {
                EventLog.WriteEntry("JBLFileWatchService", "azureDelete " + OldFilename  + " " + e.Message, EventLogEntryType.Error);
            }
            
        }

        static int runAndWait(string commandLine, string arguments, int waitMs, string workingDir)
        {
            System.Diagnostics.Process baseProcess = new System.Diagnostics.Process();
            System.Diagnostics.ProcessStartInfo processInfo = new System.Diagnostics.ProcessStartInfo();
            //string Debug = "";
            
            int processStatusCode = -1;

            if (waitMs < 1)
            {
                // set a default
                waitMs = 60000;
            }

            //EventLog.WriteEntry("JBLFileWatchService", "RunAndWait Start: cmd:" + azCopyCommand + " arg:" + azCopyArgument, EventLogEntryType.Information );

            //Debug += "2."
            //    'configure the process parameters
            processInfo.Arguments = arguments;

            //Debug += "3.";

            processInfo.CreateNoWindow = true;

            //Debug += "4." + commandLine + "." + arguments + ".";

            processInfo.FileName = commandLine;
            processInfo.RedirectStandardOutput = false;
            processInfo.UseShellExecute = false;
            processInfo.WorkingDirectory = workingDir;
            processInfo.WindowStyle = ProcessWindowStyle.Hidden;

            //Debug += processInfo.WorkingDirectory + ".";
            //    'start the process
            baseProcess = Process.Start(processInfo);
            //    'run and wait
            if (baseProcess.WaitForExit(waitMs) == true)
            {
                //Debug += "6.";
                if (baseProcess.ExitCode >= 0)
                {
                    //exited normally
                    processStatusCode = 0;
                } //else, exited w/ error and will return -1;
            }
            else
            {
                try
                {
                    processStatusCode = -2;
                    //'timed out
                    //        
                    //        'kill all sub-processes
                    massacre(baseProcess.Id);
                    baseProcess.Kill();
                }
                catch
                {

                }
            }

            baseProcess.Close();

            baseProcess = null;
            processInfo = null;
            return processStatusCode; // + Debug;
        }

        static void massacre(int processID)
        {

            ManagementObjectSearcher subProcessList;
            //ManagementObject subProcess;
            subProcessList = new ManagementObjectSearcher("SELECT * FROM Win32_Process where ParentProcessID=" + processID.ToString());
            foreach (ManagementObject subProcess in subProcessList.Get())
            {
                massacre(Convert.ToInt32(Convert.ToUInt32(subProcess["ProcessID"])));
                //kill this sub-process
                subProcess.InvokeMethod("Terminate", null);
            }
        }

    }
}

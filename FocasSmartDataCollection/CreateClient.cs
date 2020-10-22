using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Net.NetworkInformation;
using System.Reflection;
using System.Text;
using System.Threading;
using FocasLib;
using FocasLibrary;
using System.Linq;
using DTO;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Collections;
using System.Text.RegularExpressions;
using MachineConnectLicenseDTO;

namespace FocasSmartDataCollection
{
    public class CreateClient
    {
        private string ipAddress;
        private ushort portNo;
        private string machineId;
        private string interfaceId;
        private string MName;
        private short AddressPartSCountFromMacro = 0;      
        private short _CompMacroLocation = 0;
        private short _OpnMacroLocation = 0;
        bool _isMacroStringEnabled = false;
        string _toolLifeStartLocation = string.Empty;

        private short InspectionDataReadFlag = 0;
        private bool enableSMSforProgramChange = false;
        public string MachineName
        {
            get { return machineId; }
        }
        MachineSetting setting = default(MachineSetting);
        MachineInfoDTO machineDTO = default(MachineInfoDTO);
        private static string appPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
        CncMachineType _cncMachineType = CncMachineType.cncUnknown;
        string _cncSeries = string.Empty;

        //LIC details
        bool _isCNCIdReadSuccessfully = false;//**********************************TODO - change to false before check-in
        private bool _isLicenseValid = false;

        private List<SpindleSpeedLoadDTO> _spindleInfoQueue = new List<SpindleSpeedLoadDTO>();
        private List<LiveDTO> _liveDTOQueue = new List<LiveDTO>();      
        private string _operationHistoryFolderPath = string.Empty;
        private int _timeDelayMainThread = 0;
        private string _programDownloadFolder = string.Empty;
      
        private static DateTime _serviceStartedTimeStamp = DateTime.Now;
        private static DateTime _nextLicCheckedTimeStamp = _serviceStartedTimeStamp;
      
       

        private Timer _timerAlarmHistory = null;
        private Timer _timerOperationHistory = null;     
        private Timer _timerTPMTrakDataCollection = null;
       

        volatile object _lockerAlarmHistory = new object();
        volatile object _lockerOperationHistory = new object();
        volatile object _lockerTPMTrakDataCollection = new object();

        bool _isOEMVersion = false;
        List<OffsetHistoryDTO> offsetHistoryList = new List<OffsetHistoryDTO>();
        List<LiveAlarm> _liveAlarmsGlobal = new List<LiveAlarm>();
        List<int> offsetHistoryRange = new List<int>();
        List<LiveAlarm> liveAlarmsLocal = new List<LiveAlarm>();
        ServiceSettingsVals appSettings = new ServiceSettingsVals();
        bool _isWorkOrderEnabled = false;

        public CreateClient(MachineInfoDTO machine)
        {            
            this.ipAddress = machine.IpAddress;
            this.portNo = (ushort)machine.PortNo;
            this.machineId = machine.MachineId;
            this.MName = this.machineId;
            this.interfaceId = machine.InterfaceId;
            this.setting = machine.Settings;
            this.machineDTO = machine;
            appSettings = DatabaseAccess.GetServiceSettingsData();

            _operationHistoryFolderPath = appSettings.OperationHistoryFileDownloadPath;
            AddressPartSCountFromMacro = this.machineDTO.Settings.PartsCountUsingMacro;
            _programDownloadFolder = appSettings.ProgramDownloadPath;

           
            _timeDelayMainThread = (int)TimeSpan.FromSeconds(appSettings.LiveDataInterval).TotalMilliseconds;
            if (_timeDelayMainThread <= 4000) _timeDelayMainThread = 4000;

            int alaramsHistoryTimerDelay = (int)TimeSpan.FromMinutes(appSettings.AlarmDataInterval).TotalMilliseconds;
            if (alaramsHistoryTimerDelay > 0 && alaramsHistoryTimerDelay < (int)TimeSpan.FromMinutes(1).TotalMilliseconds)
                alaramsHistoryTimerDelay = (int)TimeSpan.FromMinutes(1).TotalMilliseconds;

            int operationHistoryTimerDelay = (int)TimeSpan.FromMinutes(appSettings.OperationHistoryInterval).TotalMilliseconds;
            if (operationHistoryTimerDelay > 0 && operationHistoryTimerDelay < (int)TimeSpan.FromMinutes(30).TotalMilliseconds)
                operationHistoryTimerDelay = (int)TimeSpan.FromMinutes(30).TotalMilliseconds;

           
            int tpmTrakDataCollectionTimerDelay = 0;
            int.TryParse(ConfigurationManager.AppSettings["TPMTrakDataCollectionTimeDelay"], out tpmTrakDataCollectionTimerDelay);
            if (tpmTrakDataCollectionTimerDelay > 0)
            {
                if (tpmTrakDataCollectionTimerDelay <= 10) tpmTrakDataCollectionTimerDelay = 10;
                tpmTrakDataCollectionTimerDelay = (int)TimeSpan.FromSeconds(tpmTrakDataCollectionTimerDelay).TotalMilliseconds;
            }

          
            if (alaramsHistoryTimerDelay > 0)
                _timerAlarmHistory = new Timer(GetAlarmsData, null, 3000, alaramsHistoryTimerDelay);

            if (operationHistoryTimerDelay > 0)
                _timerOperationHistory = new Timer(GetOperationHistoryData, null, 2000, operationHistoryTimerDelay);

            if (tpmTrakDataCollectionTimerDelay > 0)
                _timerTPMTrakDataCollection = new Timer(GetTPMTrakStringData, null, 1000, tpmTrakDataCollectionTimerDelay);

            
        }

        public void GetClient()
        {           
            ushort focasLibHandleMain = ushort.MinValue;
            bool IsConnected = false;

            //string _previousProgramNumber = string.Empty;
            //int _previousProgramCount;
            //DateTime _previousBatchTS;

            //int _previousMachineUpDownStatus = int.MinValue;
            //DateTime _previousCNCtimeStamp = DateTime.MinValue;
            //DateTime _previousUpDownStatusBatchTS = DateTime.MinValue;

            //DateTime _programNextDownloadTime = DateTime.Now.AddMinutes(1);
            //var plantId =  DatabaseAccess.GetPlantIDForMachine(this.machineId);
           // _programDownloadFolder = Path.Combine(_programDownloadFolder, plantId, this.machineId); 
            //DatabaseAccess.GetPartsCountAndBatchTS(this.machineId, out  _previousProgramNumber, out _previousProgramCount, out _previousBatchTS, out _previousCNCtimeStamp, out _previousMachineUpDownStatus, out _previousUpDownStatusBatchTS);
            
            Logger.WriteDebugLog(string.Format("Thread {0} started for data collection.", machineId));           

            //TODO - satya
            bool LiveDataEnabled = false;
            bool.TryParse(ConfigurationManager.AppSettings["LiveDataEnabled"].ToString(), out LiveDataEnabled);

          

            //bool enableAutoProgramDownload = false;
            //bool.TryParse(ConfigurationManager.AppSettings["EnableAutoProgramDownload"].ToString(), out enableAutoProgramDownload);


            //Int16.TryParse(ConfigurationManager.AppSettings["CompMacrolocation"].ToString(), out _CompMacroLocation);
            //Int16.TryParse(ConfigurationManager.AppSettings["OpnMacrolocation"].ToString(), out _OpnMacroLocation);
            //Int16.TryParse(ConfigurationManager.AppSettings["InspectionDataReadFlag"].ToString(), out InspectionDataReadFlag);
            //bool compareSubProgramInPATH2 = ConfigurationManager.AppSettings["CampareSubProgramInPATH2"].ToString().Equals("true", StringComparison.OrdinalIgnoreCase);
            //_isMacroStringEnabled = ConfigurationManager.AppSettings["EnableToolLifeUsingMacroString"].ToString().Equals("true", StringComparison.OrdinalIgnoreCase);
            //_toolLifeStartLocation = ConfigurationManager.AppSettings["ToolLifeMacroLocationStartingAddress"].ToString();

            short ret = 0;
            string machineStatus = string.Empty;
            string currentAmps = string.Empty;
            FocasLibBase.ODBDY2_1 dynamic_data = new FocasLibBase.ODBDY2_1();
            LiveDTO live = default(LiveDTO);
            //if (_timerOffsetHistoryReader != null)
            //{               
            //   offsetHistoryRange = GetOffsetRange();
            //   foreach (int i in offsetHistoryRange)
            //   {
            //       offsetHistoryList.Add(new OffsetHistoryDTO { MachineID = this.machineId, OffsetNo = i });
            //   }               
            //}
            _isWorkOrderEnabled = true;// DatabaseAccess.IsWorkOrderEnabled();

            while (true)
            {             
                focasLibHandleMain = ushort.MinValue;
                try
                {
                    #region stop_service                   
                    if (ServiceStop.stop_service == 1)
                    {
                        try
                        {
                            Logger.WriteDebugLog("stopping the service. coming out of main while loop.");
                            break;
                        }
                        catch (Exception ex)
                        {
                            Logger.WriteErrorLog(ex.Message);
                            break;
                        }
                    }
                    #endregion                  

                   
                    try
                    {
                        if (Utility.CheckPingStatus(this.ipAddress))
                        {                            
                            #region LicenseCheck
                            if (!_isCNCIdReadSuccessfully)
                            {
                                string cncId = string.Empty;
                                List<string> cncIdList = FocasSmartDataService.licInfo.CNCData.Where(s => s.CNCdata1 != null).Select(s => s.CNCdata1).ToList();
                                _isLicenseValid = this.ValidateCNCSerialNo(this.machineId, this.ipAddress, this.portNo, cncIdList, out _isCNCIdReadSuccessfully, out cncId);

                                if (!_isLicenseValid)
                                {
                                    if (_isCNCIdReadSuccessfully)
                                    {
                                        Logger.WriteErrorLog("Lic Validation failed. Please contact AMIT/MMT.");
                                        break;
                                    }
                                    Thread.Sleep(TimeSpan.FromSeconds(10.0));
                                    continue;
                                }
                                //update table 
                                if (_isLicenseValid)
                                {
                                    var cncDataList = FocasSmartDataService.licInfo.CNCData.Where(s => s.CNCdata1 != null && s.CNCdata1 == cncId).Select(s => s).ToList();
                                    var cncDataList2 = cncDataList.Where(s => s.IsOEM == false).Select(s => s).FirstOrDefault();
                                    if (cncDataList2 != null)
                                        _isOEMVersion = false;
                                    else
                                    {
                                        _isOEMVersion = true;
                                    }

                                    CNCData cncData = FocasSmartDataService.licInfo.CNCData.Where(s => s.CNCdata1 == cncId).Select(s => s).FirstOrDefault();
                                    cncData.IsOEM = _isOEMVersion;
                                    //_isOEMVersion = cncData.IsOEM;
                                    DatabaseAccess.UpdateMachineInfo(this.machineId, FocasSmartDataService.licInfo.LicType, FocasSmartDataService.licInfo.ExpiresAt, cncData);
                                    //this.ValidateMachineModel(this.machineId, this.ipAddress, this.portNo);
                                    this.SetCNCDateTime(this.machineId, this.ipAddress, this.portNo);

                                }
                            }
                            if (FocasSmartDataService.licInfo != null && FocasSmartDataService.licInfo.LicType != null && FocasSmartDataService.licInfo.LicType.Equals("Trial"))
                            {
                                if (_nextLicCheckedTimeStamp <= DateTime.Now)
                                {
                                    if (Utility.GetNetworkTime().Date >= DateTime.Parse(FocasSmartDataService.licInfo.ExpiresAt))
                                    {
                                        Logger.WriteErrorLog("Trial license expires. Please contact MMT/AMIT Pvt Ltd.");
                                        ServiceStop.stop_service = 1;
                                        _isLicenseValid = false;
                                        break;
                                    }
                                    int totalServiceruntime = DatabaseAccess.GetServiceRuntime();
                                    if (totalServiceruntime >= Math.Abs((DateTime.Parse(FocasSmartDataService.licInfo.StartDate) - DateTime.Parse(FocasSmartDataService.licInfo.ExpiresAt)).TotalHours))
                                    {
                                        Logger.WriteErrorLog("Trial license expires. Please contact MMT/AMIT Pvt Ltd.");
                                        ServiceStop.stop_service = 1;
                                        _isLicenseValid = false;
                                        break;
                                    }
                                    if (_serviceStartedTimeStamp != _nextLicCheckedTimeStamp)
                                    {
                                        DatabaseAccess.UpdateServiceRuntime(totalServiceruntime + 1);
                                    }
                                    _nextLicCheckedTimeStamp = DateTime.Now.AddHours(1.0);
                                }
                            }
                            #endregion
                            live = null;
                        }
                        else
                        {
                            IsConnected = false;                           
                            if (ServiceStop.stop_service == 1) break;
                            Thread.Sleep(1000 * 4);
                        }
                        //if (ping != null) ping.Dispose();
                        if (_timeDelayMainThread > 0)
                        {
                            if (ServiceStop.stop_service == 1)  break;
                            Thread.Sleep(_timeDelayMainThread);
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.WriteErrorLog("Exception inside main while loop : " + e.ToString());                      
                        Thread.Sleep(1000 * 4);
                        IsConnected = false;
                    }
                    finally
                    {                       
                        if (focasLibHandleMain != ushort.MinValue)
                        {
                            ret = FocasData.cnc_freelibhndl(focasLibHandleMain);
                            //if (ret != 0) _focasHandles.Add(focasLibHandleMain);
                            IsConnected = false;
                            Logger.WriteDebugLog("Closing connection. ret = " + ret);
                            focasLibHandleMain = ushort.MinValue;                            
                        }
                    }
                }
                catch (Exception ex)
                {
                    Logger.WriteErrorLog("Exception from main while loop : " + ex.ToString());
                    Thread.Sleep(2000);
                }
            }
            this.CloseTimer();
            Logger.WriteDebugLog("End of while loop." + Environment.NewLine + "------------------------------------------");
        }

        private void SetCNCDateTime(string machineId, string ipAddress, ushort port)
        {           
            Ping ping = null;
            ushort focasLibHandle = 0;
            try
            {
                ping = new Ping();
                PingReply pingReply = null;
                int count = 0;
                while (true)
                {
                    pingReply = ping.Send(ipAddress, 10000);
                    if (pingReply.Status != IPStatus.Success)
                    {
                        if (ServiceStop.stop_service == 1) break;
                        Logger.WriteErrorLog("Not able to ping. Ping status = " + pingReply.Status.ToString());
                        Thread.Sleep(2000);
                    }
                    else if (pingReply.Status == IPStatus.Success || ServiceStop.stop_service == 1 || count == 4)
                    {
                        break;
                    }
                    ++count;
                }
                if (pingReply.Status == IPStatus.Success)
                {
                    int num2 = FocasData.cnc_allclibhndl3(ipAddress, port, 10, out focasLibHandle);
                    if (num2 == 0)
                    {
                        FocasData.SetCNCDate(focasLibHandle, DateTime.Now);
                        FocasData.SetCNCTime(focasLibHandle, DateTime.Now);                                             
                    }
                    else
                    {
                        Logger.WriteErrorLog("Not able to connect to machine. cnc_allclibhndl3 status = " + num2.ToString());
                    }
                }
                else
                {
                    Logger.WriteErrorLog("Not able to ping. Ping status = " + pingReply.Status.ToString());
                }
            }
            catch (Exception ex)
            {
                Logger.WriteDebugLog(ex.ToString());
            }
            finally
            {
                if (ping != null)
                {
                    ping.Dispose();
                }
                if (focasLibHandle != 0)
                {
                    short num3 = FocasData.cnc_freelibhndl(focasLibHandle);                   
                }
            }            
        }

        private List<int> GetOffsetRange()
        {
            List<int> range = new List<int>();
            try
            {
                string offsetRange = ConfigurationManager.AppSettings["OffsetHistoryRange"].ToString();
                var splitRange = offsetRange.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                foreach (string item in splitRange)
                {
                    var rangeArr = item.Split(new char[] { '-' }, StringSplitOptions.RemoveEmptyEntries);

                    for (int i = int.Parse(rangeArr[0]); i <= int.Parse(rangeArr[1]); i++)
                    {
                        if (!range.Exists(t => t == i))
                        {
                            range.Add(i);
                        }
                    }
                }
            }
            catch (Exception eee)
            {
                Logger.WriteErrorLog(eee.ToString());
            }
            return range;
        }       
        

    
        private bool ValidateCNCSerialNo(string machineId, string ipAddress, ushort port, List<string> cncSerialnumbers, out bool isLicCheckedSucessfully, out string cncID)
        {
            bool result = false;
            isLicCheckedSucessfully = true;
            Ping ping = null;
            ushort focasLibHandle = 0;
            cncID = string.Empty;

            try
            {
                ping = new Ping();
                PingReply pingReply = null;
                while (true)
                {
                    pingReply = ping.Send(ipAddress, 10000);
                    if (pingReply.Status != IPStatus.Success)
                    {
                        if (ServiceStop.stop_service == 1) break;
                        Logger.WriteErrorLog("Not able to ping. Ping status = " + pingReply.Status.ToString());
                        Thread.Sleep(10000);
                    }
                    else if (pingReply.Status == IPStatus.Success || ServiceStop.stop_service == 1)
                    {
                        break;
                    }
                }
                if (pingReply.Status == IPStatus.Success)
                {
                    int num2 = FocasData.cnc_allclibhndl3(ipAddress, port, 10, out focasLibHandle);
                    if (num2 == 0)
                    {
                        string text = FocasData.ReadCNCId(focasLibHandle);
                        if (!string.IsNullOrEmpty(text))
                        {
                            if (cncSerialnumbers.Contains(text))
                            {
                                cncID = text;
                                result = true;
                            }
                        }
                        else
                        {
                            isLicCheckedSucessfully = false;
                        }
                    }
                    else
                    {
                        Logger.WriteErrorLog("Not able to connect to machine. cnc_allclibhndl3 status = " + num2.ToString());
                        isLicCheckedSucessfully = false;
                    }
                }
                else
                {
                    Logger.WriteErrorLog("Not able to ping. Ping status = " + pingReply.Status.ToString());
                    isLicCheckedSucessfully = false;
                }
            }
            catch (Exception ex)
            {
                isLicCheckedSucessfully = false;
                Logger.WriteDebugLog(ex.ToString());
            }
            finally
            {
                if (ping != null)
                {
                    ping.Dispose();
                }
                if (focasLibHandle != 0)
                {
                    short num3 = FocasData.cnc_freelibhndl(focasLibHandle);
                    //if (num3 != 0) _focasHandles.Add(focasLibHandle);
                }
            }
            return result;
        }

        private bool ValidateMachineModel(string machineId, string ipAddress, ushort port)
        {
            bool result = false;           
            Ping ping = null;
            ushort focasLibHandle = 0;
            try
            {
                ping = new Ping();
                PingReply pingReply = null;
                while (true)
                {
                    pingReply = ping.Send(ipAddress, 10000);
                    if (pingReply.Status != IPStatus.Success)
                    {
                        if (ServiceStop.stop_service == 1) break;
                        Logger.WriteErrorLog("Not able to ping. Ping status = " + pingReply.Status.ToString());
                        Thread.Sleep(10000);
                    }
                    else if (pingReply.Status == IPStatus.Success || ServiceStop.stop_service == 1)
                    {
                        break;
                    }
                }
                if (pingReply.Status == IPStatus.Success)
                {
                    int num2 = FocasData.cnc_allclibhndl3(ipAddress, port, 10, out focasLibHandle);
                    if (num2 == 0)
                    {
                        int mcModel = FocasData.ReadParameterInt(focasLibHandle, 4133);
                        int maxSpeedOnMotor = FocasData.ReadParameterInt(focasLibHandle, 4020);
                        int maxSpeedOnSpindle = FocasData.ReadParameterInt(focasLibHandle, 3741);
                        if (mcModel > 0)
                        {
                            DatabaseAccess.UpdateMachineModel(machineId, mcModel);
                        }
                    }
                    else
                    {
                        Logger.WriteErrorLog("Not able to connect to machine. cnc_allclibhndl3 status = " + num2.ToString());                       
                    }
                }
                else
                {
                    Logger.WriteErrorLog("Not able to ping. Ping status = " + pingReply.Status.ToString());                   
                }
            }
            catch (Exception ex)
            {              
                Logger.WriteDebugLog(ex.ToString());
            }
            finally
            {
                if (ping != null)
                {
                    ping.Dispose();
                }
                if (focasLibHandle != 0)
                {
                    short num3 = FocasData.cnc_freelibhndl(focasLibHandle);
                   // if (num3 != 0) _focasHandles.Add(focasLibHandle);
                }
            }
            return result;
        }

        private static List<int> FindSubPrograms(string programText)
        {
            List<int> programs = new List<int>();
            if (programText.Contains("M98P"))
            {
                string[] lines = programText.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                //parse the file to findout sub-programs
                foreach (var line in lines)
                {
                    if (line.Contains("M98P"))
                    {
                        string prg = line.Remove(0, line.IndexOf("M98P") + 4);
                        Regex rgx = new Regex("[a-zA-Z ]"); //Regex.Replace(prg,"[^0-9 ]","");                       
                        prg = rgx.Replace(prg, "");
                        int p;
                        if (Int32.TryParse(prg, out p))
                        {
                            if (!programs.Contains(p))
                            {
                                programs.Add(p);
                            }
                        }
                    }
                }
            }
            return programs;
        }

 		private static List<int> FindSubProgramsDASCNC(string programText)
        {
            List<int> programs = new List<int>();
            if (programText.Contains("M90"))
            {
                string[] lines = programText.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                //parse the file to findout sub-programs
                foreach (var line in lines)
                {
                    if (line.Contains("M90"))
                    {
                        string prg = line.Remove(0, line.IndexOf("M90") + 3);
                        //Regex rgx = new Regex("[a-zA-Z )]"); //Regex.Replace(prg,"[^0-9 ]","");      
                        Regex rgx = new Regex("[^0-9 ]");
                        prg = rgx.Replace(prg, "");
                        int p;
                        if (Int32.TryParse(prg, out p))
                        {
                            if (!programs.Contains(p))
                            {
                                programs.Add(p);
                            }
                        }
                    }
                }
            }
            return programs;
        }
        private static string FindProgramComment(string programText)
        {
            string comment = "(";
            string[] lines = programText.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var line in lines.ToList().Take(2))
            {
                if (line.Contains("(") && line.Contains(")"))
                {
                    comment += line.Substring(line.IndexOf("(") + 1, line.IndexOf(")") - line.IndexOf("(") - 1);
                    break;
                }
            }
            return Utility.SafeFileName(comment + ")");
        }

        private static string FindProgramNumberAndComment(string programText, out int programNumber)
        {
            string comment = "(";
            programNumber = 0;
            string[] lines = programText.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var line in lines.ToList().Take(2))
            {
                if (line.Contains("O"))
                {
                    string prog = line;
                    if (line.Contains("("))
                    {
                        prog = prog.Substring(prog.IndexOf("O") + 1, prog.IndexOf("(") - 1);
                    }
                    else
                    {
                        Regex rgx = new Regex("[a-zA-Z() ]"); //Regex.Replace(prg,"[^0-9 ]","");                       
                        prog = rgx.Replace(prog, "");
                    }
                    int p;
                    if (Int32.TryParse(prog, out p))
                    {
                        programNumber = p;
                    }
                    break;
                }
            }
            foreach (var line in lines.ToList().Take(2))
            {
                if (line.Contains("(") && line.Contains(")"))
                {
                    comment += line.Substring(line.IndexOf("(") + 1, line.IndexOf(")") - line.IndexOf("(") - 1);
                    break;
                }
            }
            return Utility.SafeFileName(comment + ")");
        }


        private static bool CompareContents(string str1, string str2)
        {
            if (str1.Equals(str2, StringComparison.OrdinalIgnoreCase))
                return true;

            return false;
        }

        private static string ReadFileContent(string filePath)
        {
            try
            {
                return File.ReadAllText((filePath));
            }
            catch (Exception ex)
            {
                Logger.WriteErrorLog(ex.ToString());
            }

            return string.Empty;
        }

        private static bool WriteFileContent(string filePath, string str)
        {
            try
            {
 				if (!Directory.Exists(Path.GetDirectoryName(filePath)))
                {
                    Directory.CreateDirectory(Path.GetDirectoryName(filePath));
                }
                File.WriteAllText((filePath), str);
                return true;
            }
            catch (Exception ex)
            {
                Logger.WriteErrorLog(ex.ToString());
            }

            return false;
        }
       
        public static string SafePathName(string name)
        {
            StringBuilder str = new StringBuilder( name);           

            foreach (char c in System.IO.Path.GetInvalidPathChars())
            {
                str = str.Replace(c, '_');
            }
            return str.ToString();
        }

        public static bool CreateDirectory(string masterProgramFolderPath)
        {
            var safeMasterProgramFolderPath = SafePathName(masterProgramFolderPath);
             if (!Directory.Exists(safeMasterProgramFolderPath))
            {
                try
                {
                    Directory.CreateDirectory(safeMasterProgramFolderPath);
                }
                catch(Exception ex)
                {
                    Logger.WriteErrorLog(ex.ToString());
                    return false;
                }
            }
            return true;
        }

        private int get_alarm_type(int n)
        {
            int i, res = 0;
            for (i = 0; i < 32; i++)
            {
                int n1 = n;

                res = (int)(n1 & (1 << i));
                if (res != 0)
                {
                    return (i);
                }
            }
            if (i == 32)
            {
                return -1;
            }
            return -1;
        }

        public void GetAlarmsData(Object stateObject)
        {
            if (!_isLicenseValid) return;
            if (Monitor.TryEnter(_lockerAlarmHistory, 100))
            {               
                try
                {
                    System.Threading.Thread.CurrentThread.CurrentCulture = new System.Globalization.CultureInfo("en-US");
                    Thread.CurrentThread.Name = "AlarmsHistory-" + Utility.SafeFileName(this.machineId);

                    if (Utility.CheckPingStatus(this.ipAddress))
                    {
                        CheckMachineType();
                        Logger.WriteDebugLog("Reading Alarms History data for control type." + _cncMachineType.ToString());
                        if (_cncMachineType == CncMachineType.cncUnknown) return;
                        DataTable dt = default(DataTable);
                        if (_cncMachineType == CncMachineType.Series300i ||
                            _cncMachineType == CncMachineType.Series310i ||
                            _cncMachineType == CncMachineType.Series320i ||
                             _cncMachineType == CncMachineType.Series350i ||
                            _cncMachineType == CncMachineType.Series0i)
                        {
                            dt = FocasData.ReadAlarmHistory(machineId, ipAddress, portNo);
                        }
                        else
                        {
                            //oimc,210i
                            dt = FocasData.ReadAlarmHistory18i(machineId, ipAddress, portNo);
                        }
                        DatabaseAccess.InsertAlarms(dt, machineId);
                        Logger.WriteDebugLog("Completed reading Alarms History data.");
                    }
                }
                catch (Exception ex)
                {
                    Logger.WriteDebugLog(ex.ToString());
                }
                finally
                {                   
                    Monitor.Exit(_lockerAlarmHistory);
                               
                }
            }

        }

        public void GetAlarmsDataforEndTimeUpdate()
        {           
            try
            {

                if (Utility.CheckPingStatus(this.ipAddress))
                {
                    CheckMachineType();
                    Logger.WriteDebugLog("Reading Alarms History data to update the ALARM END TIME for control type." + _cncMachineType.ToString());
                    if (_cncMachineType == CncMachineType.cncUnknown) return;
                    DataTable dt = default(DataTable);
                    if (_cncMachineType == CncMachineType.Series300i ||
                        _cncMachineType == CncMachineType.Series310i ||
                        _cncMachineType == CncMachineType.Series320i ||
                          _cncMachineType == CncMachineType.Series350i ||
                        _cncMachineType == CncMachineType.Series0i)
                    {
                        dt = FocasData.ReadAlarmHistory(machineId, ipAddress, portNo);
                    }
                    else
                    {
                        //oimc,210i
                        dt = FocasData.ReadAlarmHistory18i(machineId, ipAddress, portNo);
                    }
                    DatabaseAccess.InsertAlarms(dt, machineId);
                    Logger.WriteDebugLog("Completed reading Alarms History data.");
                }
            }
            catch (Exception ex)
            {
                Logger.WriteDebugLog(ex.ToString());
            }
            finally
            {              
               
            }

        }

        public void GetOperationHistoryData(Object stateObject)
        {
            if (!_isLicenseValid) return;
            if (Monitor.TryEnter(_lockerOperationHistory, 100))
            {
                System.Threading.Thread.CurrentThread.CurrentCulture = new System.Globalization.CultureInfo("en-US");
              
                try
                {
                    Thread.CurrentThread.Name = "OperationHistory-" + Utility.SafeFileName(this.machineId);

                    if (Utility.CheckPingStatus(this.ipAddress))
                    {
                        Logger.WriteDebugLog("Reading Operation History data for control type." + _cncMachineType.ToString());
                        string FilePath = Path.Combine(_operationHistoryFolderPath, this.machineId, DateTime.Now.ToString("yyyy-MM-dd"));
                        if (!Directory.Exists(FilePath))
                        {
                            try
                            {
                                Directory.CreateDirectory(FilePath);
                            }
                            catch { }
                        }
                        string fileName = Path.Combine(FilePath, DateTime.Now.ToString("yyyyMMddHHmmss") + ".txt");
                        short OperationHistoryFlagLocation = 0;
                        short dprint_flagLocation = 0;
                        int dprint_flagValue = 0;
                        if (!short.TryParse(ConfigurationManager.AppSettings["OperationHistory_FlagLocation"].ToString(), out OperationHistoryFlagLocation))
                        {
                            OperationHistoryFlagLocation = 0;
                        }
                        if (!short.TryParse(ConfigurationManager.AppSettings["DPRINT_FlagLocation"].ToString(), out dprint_flagLocation))
                        {
                            dprint_flagLocation = 0;
                        }


                        try
                        {
                            if (OperationHistoryFlagLocation > 0)
                            {
                                FocasData.UpdateOperatinHistoryMacroLocation(this.ipAddress, this.portNo, OperationHistoryFlagLocation, 1);
                            }
                            if (dprint_flagLocation > 0)
                            {
                                dprint_flagValue = FocasData.ReadOperatinHistoryDPrintLocation(this.ipAddress, this.portNo, dprint_flagLocation);
                            }
                            if (dprint_flagValue == 0)
                            {
                                FocasData.DownloadOperationHistory(this.ipAddress, this.portNo, fileName);
                            }
                            if (OperationHistoryFlagLocation > 0)
                            {
                                FocasData.UpdateOperatinHistoryMacroLocation(this.ipAddress, this.portNo, OperationHistoryFlagLocation, 0);
                            }
                        }
                        catch (Exception ex)
                        {
                            Logger.WriteErrorLog(ex.ToString());
                        }
                    }                   
                }
                catch (Exception ex)
                {
                    Logger.WriteDebugLog(ex.ToString());
                }
                finally
                {                   
                    Monitor.Exit(_lockerOperationHistory);                                    
                }
            }
        }
        
        public void GetTPMTrakStringData(object stateObject)
        {
            if (!_isLicenseValid) return;
            if (Monitor.TryEnter(this._lockerTPMTrakDataCollection, 100))
            {
                System.Threading.Thread.CurrentThread.CurrentCulture = new System.Globalization.CultureInfo("en-US");
                ushort focasLibHandle = 0;
                string text = string.Empty;               
                try
                {
                    Thread.CurrentThread.Name = "TPMTrakDataCollation-" + this.machineId;
                   
                    if ( Utility.CheckPingStatus(this.ipAddress))
                    {
                        Logger.WriteDebugLog("Reading Production data. ");
                        int ret = (int)FocasData.cnc_allclibhndl3(this.ipAddress, this.portNo, 10, out focasLibHandle);
                        if (ret == 0)
                        {
                            //string mode = FocasData.ReadMachineMode(focasLibHandle);
                            //if (mode.Equals("MEM", StringComparison.OrdinalIgnoreCase))
                            {
                                List<TPMString> list = new List<TPMString>();
                                foreach (TPMMacroLocation current in this.setting.TPMDataMacroLocations)
                                {
                                    int isDataReadyToread = FocasData.ReadMacro(focasLibHandle, current.StatusMacro);
                                    if (isDataReadyToread > 0)
                                    {
                                        Logger.WriteDebugLog("Reading Production data. Data read Macro location is high. = " + current.StatusMacro);
                                        List<int> values = FocasData.ReadMacroRange(focasLibHandle, current.StartLocation, current.EndLocation);
                                        TPMString tPMString = new TPMString();
                                        tPMString.Seq = values[0];
                                        text = this.BuildString(values);
                                        tPMString.TpmString = text;
                                        this.SaveStringToTPMFile(text);
                                        //tPMString.DateTime = this.GetDatetimeFromtpmString(values);

                                        list.Add(tPMString);
                                        FocasData.WriteMacro(focasLibHandle, current.StatusMacro, 0);
                                    }
                                }
                                foreach (TPMString current2 in list.OrderBy(s => s.Seq))
                                {
                                    this.ProcessData(current2.TpmString, this.ipAddress, this.portNo.ToString(), this.machineId);
                                }
                            }
                        }
                        else
                        {
                            Logger.WriteErrorLog("Not able to connect to CNC machine. ret value from fun cnc_allclibhndl3 = " + ret);
                        }
                    }                    
                }
                catch (Exception ex)
                {
                    Logger.WriteDebugLog(ex.ToString());
                }
                finally
                {
                    if (focasLibHandle != 0)
                    {
                        var r = FocasData.cnc_freelibhndl(focasLibHandle);
                        //if (r != 0) _focasHandles.Add(focasLibHandle);
                    }                    
                    Monitor.Exit(this._lockerTPMTrakDataCollection);

                }
            }
        }      

      

        private DateTime GetDatetimeFromtpmString(List<int> values)
        {
            string[] formats = new string[]
			{
				"yyyyMMdd HHmmss"
			};
            DateTime minValue = DateTime.MinValue;
            var date = values[values.Count - 2];
            var time = values[values.Count - 1];
            if (!DateTime.TryParseExact(date + " " + time, formats, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out minValue))
            {
                string strDate = Utility.get_actual_date(date);
                string strTime = Utility.get_actual_time(time);
                DateTime.TryParse(strDate + " " + strTime, out minValue);   
            }
            return minValue;
        }
        
        private string BuildString(List<int> values)
        {
            //START-1-21-[5646751-DX00]-20-opr-pallet-HeatCode-PartSL01-Supplier01-Supervisor01-20200527-111800-20200527-123000-END
            string dataString = string.Empty;
            if (values[1] == 11 || values[1] == 1 || values[1] == 22 || values[1] == 2)
            {
                var data = DatabaseAccess.getComponenetInforShanti(values[2].ToString(),values[3].ToString());
                var supiserCode = data.MachineStatus == 1 ? data.SupervisorID : values[7].ToString();
                dataString = string.Format("START-{0}-{1}-[{2}]-{3}-{4}-{5}-{6}-{7}-{8}-{9}-{10}-{11}-{12}-{13}-END-{14}",
                        values[1], values[2], data.ComponentID, values[4], values[5], values[6], data.HeatCode, data.PartSlNo, data.SupplierCode,
                       supiserCode, values[8], values[9], values[10], values[11], values[0]);
            }
            return dataString;
        }

        private string BuildStringOffset(List<int> values)
        {
            return string.Format("START-{0}-{1}-{2}-{3}-{4}-{5}-{6}-{7}-{8}-{9}-END", values[0], values[1], values[2],
                                            values[3], values[4], values[5], values[6], values[7], values[8], values[9].ToString("000000"));

            //START-Data type-MachineID-PartID-Operation-Tool no-Edge no-Target-Actual-Date-Time-END
        }

        private string BuildInspection37String(string mc, string comp, string opn, SPCCharacteristics spc, DateTime cncTime)
        {
           //START-37-MC-COMP-OPRN-Featureid-DIMENSIONid-<VALUE>-DATE-TIME-END 
            return string.Format("START-37-{0}-{1}-{2}-{3}-{4}-@{5}/-{6}-{7}-END", mc, comp,opn, spc.FeatureID, spc.DiamentionId, spc.DiamentionValue, cncTime.ToString("yyyyMMdd"), cncTime.ToString("HHmmss") );
        }

        private void SaveStringToTPMFile(string str)
        {
            string progTime = String.Format("_{0:yyyyMMdd}", DateTime.Now);

            StreamWriter writer = default(StreamWriter);
            try
            {
                writer = new StreamWriter(appPath + "\\TPMFiles\\F-" + Utility.SafeFileName(Thread.CurrentThread.Name + progTime) + ".tpm", true);
                writer.WriteLine(str);
                writer.Flush();
            }
            catch (Exception ex)
            {
                Logger.WriteErrorLog(ex.Message);
            }
            finally
            {
                if (writer != null)
                {
                    writer.Close();
                    writer.Dispose();
                }
            }
        }

        public void WriteInToFileDBInsert(string str)
        {
            string progTime = String.Format("_{0:yyyyMMdd}", DateTime.Now);
            string location = appPath + "\\Logs\\DBInsert-" + Utility.SafeFileName( MName + progTime) + ".txt";

            StreamWriter writer = default(StreamWriter);
            try
            {
                writer = new StreamWriter(location, true, Encoding.Default, 8195);
                writer.WriteLine(str);
                writer.Flush();
            }
            catch (Exception ex)
            {
                Logger.WriteErrorLog(ex.Message);
            }
            finally
            {
                if (writer != null)
                {
                    writer.Close();
                    writer.Dispose();
                }
            }
        }

        public void ProcessData(string InputStr, string IP, string PortNo, string MName)
        {
            try
            {
                string ValidString = FilterInvalids(InputStr);
                WriteInToFileDBInsert(string.Format("{0} : Start Insert Record - {1} ; IP = {2}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss:FFF"), ValidString, IP));
                InsertDataUsingSP(ValidString, IP, PortNo);
                WriteInToFileDBInsert(string.Format("{0} : Stop Insert - {1}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss:FFF"), IP));
            }
            catch (Exception ex)
            {
                Logger.WriteErrorLog("ProcessFile() :" + ex.ToString());
            }
            return;
        }

        public static string FilterInvalids(string DataString)
        {
            string FilterString = string.Empty;
            try
            {
                for (int i = 0; i < DataString.Length; i++)
                {
                    byte[] asciiBytes = Encoding.ASCII.GetBytes(DataString.Substring(i, 1));

                    if (asciiBytes[0] >= Encoding.ASCII.GetBytes("#")[0] && asciiBytes[0] <= Encoding.ASCII.GetBytes("}")[0])  //to handle STR   -1-0111-000000001-1-0002-1-20110713-175258914-20110713-175847898-END more than 2 spaces in string
                    {
                        FilterString = FilterString + DataString.Substring(i, 1);
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.WriteErrorLog(ex.Message);
            }
            return FilterString;
        }

        public static int InsertDataUsingSP(string DataString, string IP, string PortNo)
        {
            SqlConnection Con = ConnectionManager.GetConnection();
            SqlCommand cmd = new SqlCommand("s_GetProcessDataString", Con);
            cmd.CommandType = CommandType.StoredProcedure;

            cmd.Parameters.Add("@datastring", SqlDbType.NVarChar).Value = DataString;
            cmd.Parameters.Add("@IpAddress", SqlDbType.NVarChar).Value = IP;
            cmd.Parameters.Add("@OutputPara", SqlDbType.Int).Value = 0;
            cmd.Parameters.Add("@LogicalPortNo", SqlDbType.SmallInt).Value = PortNo;
            int OutPut = 0;
            try
            {
                OutPut = cmd.ExecuteNonQuery();
                if (OutPut < 0)
                {
                    Logger.WriteErrorLog(string.Format("InsertDataUsingSP() - ExecuteNonQuery returns < 0 value : {0} :- {1}", IP, DataString));
                }
            }
            catch (Exception ex)
            {
                Logger.WriteErrorLog("InsertDataUsingSP():" + ex.Message);
            }
            finally
            {
                if (Con != null) Con.Close();
                cmd = null;
                Con = null;
            }
            return OutPut;
        }          
          
        public void CloseTimer()
        {
            if (_timerAlarmHistory != null) _timerAlarmHistory.Dispose();
            if (_timerOperationHistory != null) _timerOperationHistory.Dispose();
            //if (_timerSpindleLoadSpeed != null) _timerSpindleLoadSpeed.Dispose();
            //if (_timerPredictiveMaintenanceReader != null) _timerPredictiveMaintenanceReader.Dispose();
            //if (this._timerTPMTrakDataCollection != null) this._timerTPMTrakDataCollection.Dispose();
            //if (this._timerOffsetHistoryReader != null) this._timerOffsetHistoryReader.Dispose();
            //if (this._timerCycletimeReader != null) this._timerCycletimeReader.Dispose();
            //if (_timerToolLife != null) this._timerToolLife.Dispose();
            //if (_timerProgramTransferPullPush != null) this._timerProgramTransferPullPush.Dispose();
            //if (_timerOffsetCorrection != null) _timerOffsetCorrection.Dispose();
        }   

        public void CheckMachineType()
        {
            if (_cncSeries.Equals(string.Empty))
            {
                ushort focasLibHandle = ushort.MinValue;
                short ret = FocasData.cnc_allclibhndl3(ipAddress, portNo, 4, out focasLibHandle);
                if (ret == 0)
                {
                    if (FocasData.GetFanucMachineType(focasLibHandle, ref _cncMachineType, out _cncSeries) != 0)
                    {
                        Logger.WriteErrorLog("Failed to get system info. method failed cnc_sysinfo()");
                    }
                    Logger.WriteDebugLog("CNC control type  = " + _cncMachineType.ToString() + " , " + _cncSeries);
                }
                ret = FocasData.cnc_freelibhndl(focasLibHandle);
                //if (ret != 0) _focasHandles.Add(focasLibHandle);
            }
        }        
    }
}


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DTO
{
    public class OffserCorrectionDTO
    {
        public int Result { get; set; }
        public decimal OffsetCorrectionValue { get; set; }
        public decimal MeasuredValue { get; set; }
       // public int OffsetCorrectionMasterID { get; set; }
        public int SampleID { get; set; }
        public short OffsetCorrectionMacroLocation { get; set; }
        public string CharacteristicID { get; set; }
        //Program id = Component
        public string ProgramID { get; set; }
        public string ResultText { get; set; }
        public short WearOffsetNumber { get; set; }
        public short WearOffsetNumberMacro { get; set; }      
        public short UniqueIDMacroLocation { get; set; }
        public short WearOffsetFlagMacro { get; set; }

        public short UniqueIDAckMacroLocation { get; set; }       
        public short WearOffsetAckFlagMacro { get; set; }

        public OffserCorrectionDTO()
        {
            this.Result = int.MinValue;
        }

        
    }
}

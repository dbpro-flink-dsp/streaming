package impro.util;


import org.apache.flink.api.common.functions.RichMapFunction;
import impro.data.GDELTGkgData;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ParseGdeltGkgDataToBin extends RichMapFunction<String, GDELTGkgData> {
    private static final long serialVersionUID = 1L;

    public ParseGdeltGkgDataToBin(){ }

    @Override
    public GDELTGkgData map(String record) throws ParseException {

        String rawData = record;
        // System.out.println("         RECORD: " + record);
        String[] data = rawData.split("\\t");
        //System.out.println("DATA Length: " + data.length);

        String gkgRecordId = data[0];
        Date V21Date = new SimpleDateFormat("yyyyMMddHHmmss").parse(data[1]);
        String V2SourceCommonName = data[3];
        String V2DocumentIdentifier = data[4];
        String V1Themes = data[7];
        String V2EnhancedThemes = data[8];
        String V1Locations = data[9];
        String V2EnhancedLocations = data[10];
        String V1Persons = data[11];
        String V2EnhancedPersons = data[12];
        String V1Organizations = data[13];
        String V2EnhancedOrganizations = data[14];
        Double V15Tone = Double.parseDouble(data[15].split(",")[0]);


        return new GDELTGkgData("gdelt_gkg", gkgRecordId, V21Date, V2SourceCommonName, V2DocumentIdentifier,
                V1Themes, V2EnhancedThemes, V1Locations, V2EnhancedLocations, V1Persons, V2EnhancedPersons,
                V1Organizations, V2EnhancedOrganizations, V15Tone);

    }
}

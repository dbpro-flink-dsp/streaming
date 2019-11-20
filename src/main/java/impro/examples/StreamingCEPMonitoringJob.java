package impro.examples;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import impro.data.GDELTGkgData;
import impro.util.ParseGdeltGkgDataToBin;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * --input ./src/main/resources/gkg_example_50.csv
 */
public class StreamingCEPMonitoringJob {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // GKG Table
        DataStream<GDELTGkgData> gdeltGkgData = env.readTextFile(params.get("input"))
                .map(new ParseGdeltGkgDataToBin())
                .assignTimestampsAndWatermarks(new GkgDataAssigner());

        DataStream<GDELTGkgData> GkgOrganizationsData = gdeltGkgData.filter(new FilterOrganisations());
        //GkgOrganizationsData.print();

        Pattern<GDELTGkgData, ?> warningPattern  =  Pattern.<GDELTGkgData>begin("first")
                .where(new IterativeCondition<GDELTGkgData>() {
                    @Override
                    public boolean filter(GDELTGkgData event, Context<GDELTGkgData> context) throws Exception {
                        String themes = event.getV1Themes();
                        if (themes.contains("ECON_") || themes.contains("ENV_")) {
                            //if (themes.contains("ECON_") ) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                }).within(Time.days(5));


        PatternStream<GDELTGkgData> patternMessageTypeWarning = CEP.pattern(GkgOrganizationsData, warningPattern);

        DataStream<Tuple4<String, String, String,String>> warnings = patternMessageTypeWarning.select(new GenerateMessageTypeWarning());

        warnings.print();

        env.execute("");
    }

    private static class GenerateMessageTypeWarning implements PatternSelectFunction<GDELTGkgData, Tuple4<String,String, String,String>> {
        @Override
        public Tuple4<String, String, String,String> select(Map<String, List<GDELTGkgData>> pattern) throws Exception {
            GDELTGkgData first = (GDELTGkgData) pattern.get("first").get(0);

            Date date = first.getV21Date();
            // filter the Themes
            String themes = first.getV1Themes();
            String orgThemes = "";
            for(String theme : themes.split(";")) {
                if(theme.startsWith("ECON_") || theme.startsWith("ENV_") ) {
                    orgThemes = orgThemes + theme + ", ";
                }
            }
            // remove the last ,
            if(orgThemes.length() > 1)
                orgThemes = orgThemes.substring(0, orgThemes.length()-2);

            //System.out.println("  WARN:" + first.toString());
            return new Tuple4<String,String,String,String>("WARNING EVENT", date.toString(), first.getV1Organizations(), orgThemes);
        }
    }

    public static class FilterOrganisations implements FilterFunction<GDELTGkgData> {
        @Override
        public boolean filter(GDELTGkgData event) {
            String orgList = event.getV1Organizations();
            if (orgList.contains("siemens") ||
                    orgList.contains("huawei") ||
                    orgList.contains("volkswagen") ||
                    orgList.contains("samsung") ||
                    orgList.contains("dupont") ||
                    orgList.contains("lufthansa") ||
                    orgList.contains("airbus") ||
                    orgList.contains("toyota")) {
                return true;
            } else {
                return false;
            }
        }
    }




    static class GkgDataAssigner implements AssignerWithPunctuatedWatermarks<GDELTGkgData> {
        @Override
        public long extractTimestamp(GDELTGkgData event, long previousElementTimestamp) {
            return event.getTimeStampMs();
        }

        @Override
        public Watermark checkAndGetNextWatermark(GDELTGkgData event, long extractedTimestamp) {
            // simply emit a watermark with every event
            return new Watermark(extractedTimestamp - 20000   );
            //return new Watermark(extractedTimestamp);
        }
    }

}


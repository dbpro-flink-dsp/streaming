package impro.data;

import java.util.Date;

public class GDELTGkgData {

    public String key;
    private String gkgRecordId;
    // TODO: adapt the right variable types
    private Date V21Date;
    //private String V2SourceCollectionIdentifier;
    private String V2SourceCommonName;
    private String V2DocumentIdentifier;
    //private String V1Counts;
    //private String V21Counts;
    private String V1Themes;
    private String V2EnhancedThemes;
    private String V1Locations;
    private String V2EnhancedLocations;
    private String V1Persons;
    private String V2EnhancedPersons;
    private String V1Organizations;
    private String V2EnhancedOrganizations;
    private Double V15Tone;
    /*private String V21EnhancedDates;
    private String V2GCAM;
    private String V21SharingImage;
    private String V21RelatedImages;
    private String V21SocialImageEmbeds;
    private String V21SocialVideoEmbeds;
    private String V21Quotations;
    private String V21AllNames;
    private String V21Amounts;
    private String V21TranslationInfo;
    private String V2ExtrasXml;*/


    public GDELTGkgData(
            String key,
            String gkgRecordId,
            // TODO: adapt the right variable types
            Date V21Date,
            //String V2SourceCollectionIdentifier,
            String V2SourceCommonName,
            String V2DocumentIdentifier,
            /*String V1Counts,
            String V21Counts,*/
            String V1Themes,
            String V2EnhancedThemes,
            String V1Locations,
            String V2EnhancedLocations,
            String V1Persons,
            String V2EnhancedPersons,
            String V1Organizations,
            String V2EnhancedOrganizations,
            Double V15Tone/*,
            String V21EnhancedDates,
            String V2GCAM,
            String V21SharingImage,
            String V21RelatedImages,
            String V21SocialImageEmbeds,
            String V21SocialVideoEmbeds,
            String V21Quotations,
            String V21AllNames,
            String V21Amounts,
            String V21TranslationInfo,
            String V2ExtrasXml*/) {
        this.key = key;
        this.gkgRecordId = gkgRecordId;
        // TODO: adapt the right variable types
        this.V21Date = V21Date;
        //this.V2SourceCollectionIdentifier = V2SourceCollectionIdentifier;
        this.V2SourceCommonName = V2SourceCommonName;
        this.V2DocumentIdentifier = V2DocumentIdentifier;
        /*this.V1Counts = V1Counts;
        this.V21Counts = V21Counts;*/
        this.V1Themes = V1Themes;
        this.V2EnhancedThemes = V2EnhancedThemes;
        this.V1Locations = V1Locations;
        this.V2EnhancedLocations = V2EnhancedLocations;
        this.V1Persons = V1Persons;
        this.V2EnhancedPersons = V2EnhancedPersons;
        this.V1Organizations = V1Organizations;
        this.V2EnhancedOrganizations = V2EnhancedOrganizations;
        this.V15Tone = V15Tone;
        /*this.V21EnhancedDates = V21EnhancedDates;
        this.V2GCAM = V2GCAM;
        this.V21SharingImage = V21SharingImage;
        this.V21RelatedImages = V21RelatedImages;
        this.V21SocialImageEmbeds = V21SocialImageEmbeds;
        this.V21SocialVideoEmbeds = V21SocialVideoEmbeds;
        this.V21Quotations = V21Quotations;
        this.V21AllNames = V21AllNames;
        this.V21Amounts = V21Amounts;
        this.V21TranslationInfo = V21TranslationInfo;
        this.V2ExtrasXml = V2ExtrasXml;*/
    }

    public long getTimeStampMs() {
        return V21Date.getTime();
    }

    public String getKey() {
        return key;
    }
    public Date getV21Date() {
        return V21Date;
    }
    public String getV1Themes() {
        return V1Themes;
    }
    public String getV1Organizations() { return V1Organizations; }

    public String getV2SourceCommonName() { return V2SourceCommonName; }

    public String getGkgRecordId() { return gkgRecordId; }

    public Double getV15Tone() { return V15Tone; }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(gkgRecordId).append(",");
        sb.append(V21Date).append(",");
        sb.append(V2SourceCommonName).append(",");
        sb.append(V2DocumentIdentifier).append(",");
        sb.append(V1Themes).append(",");
        sb.append(V2EnhancedThemes).append(",");
        sb.append(V1Locations).append(",");
        sb.append(V2EnhancedLocations).append(",");
        sb.append(V1Persons).append(",");
        sb.append(V2EnhancedPersons).append(",");
        sb.append(V1Organizations).append(",");
        sb.append(V2EnhancedOrganizations).append(",");
        sb.append(V15Tone);

        return sb.toString();
    }
}


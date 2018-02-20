package org.broadinstitute.cromstream;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.OffsetDateTime;

public class MetadataEvent {
    public MetadataKey key;
    public MetadataValue value;
    public OffsetDateTime offsetDateTime;
}

@JsonIgnoreProperties(ignoreUnknown=true)
class MetadataKey {
    public String workflowId;
    public MetadataJobKey jobKey;
    public String key;

}

@JsonIgnoreProperties(ignoreUnknown=true)
class MetadataJobKey {
    public String callFqn;
    public Integer index;
    public Integer attempt;
}

@JsonIgnoreProperties(ignoreUnknown=true)
class MetadataValue {
    public String value;
    public String valueType;
}

use crate::protocols::get_current_time_millis;
use serde::Serialize;

#[derive(Debug, Serialize, Eq, PartialEq, Hash)]
#[allow(non_snake_case)]
pub struct SubscriptionData {
    //   private boolean classFilterMode = false;
    //     private String topic;
    //     private String subString;
    //     private Set<String> tagsSet = new HashSet<String>();
    //     private Set<Integer> codeSet = new HashSet<Integer>();
    //     private long subVersion = System.currentTimeMillis();
    //     private String expressionType = ExpressionType.TAG;
    //
    //     @JSONField(serialize = false)
    //     private String filterClassSource;
    pub classFilterMode: bool,
    pub topic: String,
    pub subString: String,
    pub tagsSet: Vec<String>,
    pub codeSet: Vec<i32>,
    pub subVersion: i64,
    pub expressionType: String,
    pub filterClassSource: String,
}

impl SubscriptionData {
    pub fn simple_new(topic: String) -> Self {
        SubscriptionData {
            classFilterMode: false,
            topic,
            subString: "*".to_string(),
            tagsSet: vec![],
            codeSet: vec![],
            subVersion: get_current_time_millis(),
            expressionType: "TAG".to_string(),
            filterClassSource: "".to_string(),
        }
    }
}

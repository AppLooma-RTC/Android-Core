# Kotlin Serialization Proguard Rules
########################################

-keepattributes *Annotation*, InnerClasses
-dontnote kotlinx.serialization.AnnotationsKt # core serialization annotations

# kotlinx-serialization-json specific. Add this if you have java.lang.NoClassDefFoundError kotlinx.serialization.json.JsonObjectSerializer
-keepclassmembers class kotlinx.serialization.json.** {
    *** Companion;
}
-keepclasseswithmembers class kotlinx.serialization.json.** {
    kotlinx.serialization.KSerializer serializer(...);
}

-keep,includedescriptorclasses class com.applooma.rtc.transport.**$$serializer { *; }
-keepclassmembers class com.applooma.rtc.transport.** {
    *** Companion;
}
-keepclasseswithmembers class com.applooma.rtc.transport.** {
    kotlinx.serialization.KSerializer serializer(...);
}

# WebRTC
#########################################
-keep class org.webrtc.** { *; }

# NIST sdp parser
#########################################
-keep class android.gov.nist.** { *; }
-dontwarn com.sun.nio.sctp.**
-dontwarn org.apache.log4j.**

# Protobuf
#########################################
-keep class * extends com.google.protobuf.GeneratedMessageLite { *; }

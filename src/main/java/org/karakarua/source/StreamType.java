package org.karakarua.source;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.karakarua.domain.UserInfo;

/**
 * 什么能被转化成流？
 * Flink 的 Java 和 Scala DataStream API 可以将任何可序列化的对象转化为流。Flink 自带的序列化器有
 * <ul>
 *     <li>基本类型，即 String、Long、Integer、Boolean、Array</li>
 *     <li>复合类型：Tuples、POJOs 和 Scala case classes</li>
 * </ul>
 * 而且 Flink 会交给 Kryo 序列化其他类型。也可以将其他序列化器和 Flink 一起使用。特别是有良好支持的 Avro。
 * <a href="https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/learn-flink/datastream_api/" >什么能被转化成流？</a>
 */
public class StreamType {
    public static void main(String[] args) {
        // Flink自带的类型序列化后可以转换为流
        TypeInformation<Tuple> tuple = Types.TUPLE(Types.STRING, Types.INT);
        // 输出 class org.apache.flink.api.java.tuple.Tuple2
        System.out.println(tuple.getTypeClass());

        // 自定义POJO交由Flink 序列化后可以转换为流
        TypeInformation<UserInfo> userType = TypeInformation.of(UserInfo.class);
        // 输出 class org.karakarua.domain.UserInfo
        System.out.println(userType.getTypeClass());
        TypeInformation<UserInfo> userTypeHint = TypeInformation.of(new TypeHint<UserInfo>() {
            @Override
            public TypeInformation<UserInfo> getTypeInfo() {
                return super.getTypeInfo();
            }
        });
        // 输出 class org.karakarua.domain.UserInfo
        System.out.println(userTypeHint.getTypeClass());
        TypeInformation<UserInfo> userTypeHint2 = new TypeHint<UserInfo>() {
            @Override
            public TypeInformation<UserInfo> getTypeInfo() {
                return super.getTypeInfo();
            }
        }.getTypeInfo();
        // 输出 class org.karakarua.domain.UserInfo
        System.out.println(userTypeHint.getTypeClass());
        // 输出 field 的个数
        System.out.println(userTypeHint.getArity());

        // 一个TypeInformation的几个关键点：使用的class类型、使用的序列化工具（可用于I/O传输）、使用的比较器，
        // 以Flink的String类型为例，它属于BasicTypeInfo，对应于BasicTypeInfo.STRING_TYPE_INFO ，
        // 使用了Java的String类型、flink定义的序列化工具StringSerializer和flink定义的比较器StringComparator
        /*
         *         public static final TypeInformation<String> STRING = BasicTypeInfo.STRING_TYPE_INFO;
         *         public static final BasicTypeInfo<String> STRING_TYPE_INFO = new BasicTypeInfo<>(
         *                         String.class,
         *                         new Class<?>[] {},
         *                         StringSerializer.INSTANCE,
         *                         StringComparator.class);
         */
        TypeInformation<String> string = Types.STRING;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.datagen;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Random;

final class BsegRow {
    static final long MAX_SEQUENCE = 9_999_999_999L * 999;
    private final Config config;
    private final Random random;
    private final String date;

    BsegRow(Config config, Random random) {
        this.config = config;
        this.random = random;
        this.date = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);
    }

    void bindInsert(PreparedStatement statement, long sequence) throws SQLException {
        checkSequence(sequence);
        BigDecimal amount = BigDecimal.valueOf(10_000L + random.nextInt(99_000_000), 2);
        boolean debit = random.nextBoolean();
        int index = 1;
        for (BsegSchema.Column column : BsegSchema.COLUMNS) {
            if (column.numeric()) {
                BigDecimal value;
                if (column.sapType.equals("CURR")) {
                    value = amount;
                }
                else if (column.name.equals("KURSR")) {
                    value = BigDecimal.ONE.setScale(column.scale);
                }
                else {
                    int bound = (int) Math.pow(10, Math.min(column.length, 6));
                    value = BigDecimal.valueOf(random.nextInt(bound), column.scale);
                }
                statement.setBigDecimal(index++, value);
                continue;
            }
            String value;
            switch (column.name) {
                case "MANDT":
                    value = config.mandt;
                    break;
                case "BUKRS":
                    value = config.bukrs;
                    break;
                case "BELNR":
                    value = document(sequence);
                    break;
                case "GJAHR":
                    value = config.gjahr;
                    break;
                case "BUZEI":
                    value = item(sequence);
                    break;
                case "BSCHL":
                    value = debit ? "40" : "50";
                    break;
                case "KOART":
                    value = "S";
                    break;
                case "SHKZG":
                    value = debit ? "S" : "H";
                    break;
                case "SGTXT":
                    value = "Generated BSEG item " + sequence;
                    break;
                case "PSWSL":
                    value = "CNY";
                    break;
                case "MWSKZ":
                case "MWSK1":
                case "MWSK2":
                case "MWSK3":
                    value = "V1";
                    break;
                case "KOKRS":
                case "GSBER":
                case "WERKS":
                case "BWKEY":
                    value = config.bukrs;
                    break;
                case "SAKNR":
                case "HKONT":
                    value = pad(600_000 + random.nextInt(10_000), 10);
                    break;
                case "MEINS":
                case "ERFME":
                case "BPRME":
                    value = "EA";
                    break;
                case "ZTERM":
                    value = "0001";
                    break;
                case "VORGN":
                    value = "RFBU";
                    break;
                case "LANDL":
                case "EGBLD":
                case "EGLLD":
                    value = "CN";
                    break;
                default:
                    if (column.sapType.equals("DATS")) {
                        value = date;
                    }
                    else if (column.length == 1 && !column.sapType.equals("NUMC")) {
                        value = column.name.startsWith("X") && random.nextBoolean() ? "X" : " ";
                    }
                    else {
                        int bound = (int) Math.pow(10, Math.min(column.length, 6));
                        value = pad(random.nextInt(bound), column.length);
                    }
            }
            statement.setString(index++, value);
        }
    }

    void bindKey(PreparedStatement statement, int firstIndex, long sequence) throws SQLException {
        statement.setString(firstIndex, config.mandt);
        statement.setString(firstIndex + 1, config.bukrs);
        statement.setString(firstIndex + 2, document(sequence));
        statement.setString(firstIndex + 3, config.gjahr);
        statement.setString(firstIndex + 4, item(sequence));
    }

    static String document(long sequence) {
        checkSequence(sequence);
        return pad((sequence - 1) / 999 + 1, 10);
    }

    static String item(long sequence) {
        checkSequence(sequence);
        return pad((sequence - 1) % 999 + 1, 3);
    }

    static long sequence(String document, String item) {
        String d = document.trim();
        String i = item.trim();
        if (!d.matches("[0-9]{10}") || !i.matches("[0-9]{3}")) {
            throw new IllegalArgumentException("选定 SAP 命名空间内的 BELNR/BUZEI 必须分别为 10/3 位数字");
        }
        long documentNumber = Long.parseLong(d);
        long itemNumber = Long.parseLong(i);
        if (documentNumber < 1 || itemNumber < 1 || itemNumber > 999) {
            throw new IllegalArgumentException("BELNR/BUZEI 必须 > 0，且 BUZEI <= 999");
        }
        long result = (documentNumber - 1) * 999 + itemNumber;
        checkSequence(result);
        return result;
    }

    private static void checkSequence(long sequence) {
        if (sequence < 1 || sequence > MAX_SEQUENCE) {
            throw new IllegalArgumentException("BSEG 凭证序号超出可用范围: " + sequence);
        }
    }

    private static String pad(long number, int length) {
        String value = Long.toString(number);
        if (value.length() > length) {
            throw new IllegalArgumentException("字段长度不足: " + number);
        }
        return "0".repeat(length - value.length()) + value;
    }
}

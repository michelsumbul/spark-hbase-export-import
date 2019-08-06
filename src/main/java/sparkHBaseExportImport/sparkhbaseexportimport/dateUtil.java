/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparkHBaseExportImport.sparkhbaseexportimport;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author msumbul
 */
public class dateUtil {

    public static String convertStringDateFormat(String inputDate, String inputFormat, String outputFormat) {
        try {
            SimpleDateFormat formatter = new SimpleDateFormat(inputFormat, Locale.ENGLISH);

            Date date = formatter.parse(inputDate);

            formatter = new SimpleDateFormat(outputFormat);
            String outputStringDate = formatter.format(date);
            return outputStringDate;
        } catch (ParseException ex) {
            Logger.getLogger(dateUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public static String convertStringToUnixtime(String inputDate, String inputFormat) {
        try {
            SimpleDateFormat formatter = new SimpleDateFormat(inputFormat, Locale.ENGLISH);

            Long date = formatter.parse(inputDate).getTime();

            return date.toString();
        } catch (ParseException ex) {
            Logger.getLogger(dateUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public static long convertStringToUnixtimeInLong(String inputDate, String inputFormat) {
        Long date = null;
        try {
            SimpleDateFormat formatter = new SimpleDateFormat(inputFormat, Locale.ENGLISH);

            date = formatter.parse(inputDate).getTime();

        } catch (ParseException ex) {
            Logger.getLogger(dateUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        return date;
    }

    public static String convertUnixtimeToString(Long unixtime) {
        java.util.Date time = new java.util.Date(unixtime);
        return time.toString();
    }

    public static String getNow(String format) {
        SimpleDateFormat sdfDate = new SimpleDateFormat(format);//dd/MM/yyyy
        Date now = new Date();

        String strDate = sdfDate.format(now);
        return strDate;
    }

    public static long getNowUnixtimeMilli() {
        Date now = new Date();
        return now.getTime();
    }
}

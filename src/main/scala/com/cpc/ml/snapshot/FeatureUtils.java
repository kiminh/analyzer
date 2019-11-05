package com.cpc.ml.snapshot;


import java.util.ArrayList;
import java.util.List;

/**
 *  created by xiongyao on 2019/11/5
 */
public class FeatureUtils {

    public ArrayList<String> getFeature_str_list(int index, List<Integer> feature_str_offset, List<String> feature_str_list){

        ArrayList<String> app_fea_strs = new ArrayList<>();

        if (index >= 0){
            int left_offset = feature_str_offset.get(index);
            int right_offset = feature_str_offset.size();
            if (index < feature_str_offset.size() - 1){
                right_offset = feature_str_offset.get(index + 1);
            }

            for(int i = left_offset; i< right_offset; i++){
                app_fea_strs.add(feature_str_list.get(i));
            }
        }
        return app_fea_strs;
    }

    public ArrayList<Integer> getFeature_int32_list(int index, ArrayList<Integer> feature_str_offset, ArrayList<Integer> feature_str_list){

        ArrayList<Integer> app_fea_strs = new ArrayList<>();

        if (index >= 0){
            int left_offset = feature_str_offset.get(index);
            int right_offset = feature_str_offset.size();
            if (index < feature_str_offset.size() - 1){
                right_offset = feature_str_offset.get(index + 1);
            }

            for(int i = left_offset; i< right_offset; i++){
                app_fea_strs.add(feature_str_list.get(i));
            }
        }
        return app_fea_strs;
    }

    public ArrayList<Long> getFeature_int64_list(int index, ArrayList<Integer> feature_str_offset, ArrayList<Long> feature_str_list){

        ArrayList<Long> app_fea_strs = new ArrayList<>();

        if (index >= 0){
            int left_offset = feature_str_offset.get(index);
            int right_offset = feature_str_offset.size();
            if (index < feature_str_offset.size() - 1){
                right_offset = feature_str_offset.get(index + 1);
            }

            for(int i = left_offset; i< right_offset; i++){
                app_fea_strs.add(feature_str_list.get(i));
            }
        }
        return app_fea_strs;
    }
}

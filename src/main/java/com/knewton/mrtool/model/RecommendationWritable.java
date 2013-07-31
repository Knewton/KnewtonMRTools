/**
 * Copyright (c) 2013 Knewton
 * 
 * Dual licensed under: MIT: http://www.opensource.org/licenses/mit-license.php GPLv3:
 * http://www.opensource.org/licenses/gpl-3.0.html.
 * 
 */
package com.knewton.mrtool.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Sample model class for representing recommendations.
 * 
 * @author Giannis Neokleous
 * 
 */
public class RecommendationWritable implements Writable {

    private String type;
    private int recId;
    private String courseId;
    private String module;
    private String filename;

    public RecommendationWritable() {
    }

    public RecommendationWritable(String type, int recId, String courseId, String module) {
        this.type = type;
        this.recId = recId;
        this.courseId = courseId;
        this.module = module;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(type);
        out.writeInt(recId);
        out.writeUTF(courseId);
        out.writeUTF(module);
        out.writeBoolean(filename == null);
        if (filename != null) {
            out.writeUTF(filename);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        type = in.readUTF();
        recId = in.readInt();
        courseId = in.readUTF();
        module = in.readUTF();
        boolean nullFilename = in.readBoolean();
        if (!nullFilename) {
            this.filename = in.readUTF();
        }
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getRecId() {
        return recId;
    }

    public void setRecId(int recId) {
        this.recId = recId;
    }

    public String getCourseId() {
        return courseId;
    }

    public void setCourseId(String courseId) {
        this.courseId = courseId;
    }

    public String getModule() {
        return module;
    }

    public void setModule(String module) {
        this.module = module;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String toString() {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("type=").append(type)
                .append(", recId=").append(recId)
                .append(", courseId").append(courseId)
                .append(", module=").append(module)
                .append(", filename=").append(filename);
        return strBuilder.toString();
    }

}

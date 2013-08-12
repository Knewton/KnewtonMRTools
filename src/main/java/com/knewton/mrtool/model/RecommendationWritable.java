/**
 * Copyright 2013 Knewton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
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

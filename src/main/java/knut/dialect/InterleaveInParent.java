/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package knut.dialect;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.persistence.Entity;

/**
 * This annotation annotates an {@link Entity} class that should be interleaved in a parent table.
 * This annotation is Cloud Spanner specific and is only used when automatic schema generation is
 * used. If you create your schema manually, you may leave this annotation out.
 *
 * To generate the following schema:
 *
 * <pre>
 * CREATE TABLE ParentTable (ParentId INT64, Name STRING(MAX)) PRIMARY KEY (ParentId);
 * CREATE TABLE ChildTable (ParentId INT64, ChildId INT64, ChildName STRING(MAX))
 *              PRIMARY KEY (ParentId, ChildId),
 *              INTERLEAVE IN PARENT ParentTable
 * </pre>
 *
 * The following Java definition should be used:
 *
 * <pre>
 * &#64;Entity
 * &#64;Table(name = "ParentTable")
 * public class Parent {
 *   &#64;Id
 *   private Long parentId;
 *
 *   &#64;Column
 *   private String name;
 *   ...
 * }
 *
 * &#64;Entity
 * &#64;Table(name = "ChildTable")
 * &#64;InterleaveInParent("ParentTable")
 * public class Child {
 *   public static class ChildId implements Serializable {
 *     private Long parentId;
 *     private Long childId;
 *     ...
 *   }
 *
 *   &#64;EmbeddedId
 *   private ChildId id;
 *
 *   &#64;Column
 *   private String ChildName
 *   ...
 * }
 * </pre>
 *
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface InterleaveInParent {
  /** @return the name of the parent table */
  String value();

  /** @return <code>true</code> if ON DELETE CASCADE should be added to the CREATE TABLE string */
  boolean cascadeDelete() default false;

}

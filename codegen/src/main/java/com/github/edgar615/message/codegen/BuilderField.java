package com.github.edgar615.message.codegen;

import javax.lang.model.type.TypeMirror;

public class BuilderField {

  private final String name;

  private final TypeMirror typeMirror;

  private final boolean notNull;

  BuilderField(String name, TypeMirror typeMirror, boolean notNull) {
    this.name = name;
    this.typeMirror = typeMirror;
    this.notNull = notNull;
  }

  public static BuilderField nullable(String name, TypeMirror typeMirror) {
    return new BuilderField(name, typeMirror, false);
  }

  public static BuilderField notNull(String name, TypeMirror typeMirror) {
    return new BuilderField(name, typeMirror, true);
  }

  public String name() {
    return name;
  }

  public TypeMirror typeMirror() {
    return typeMirror;
  }

  public boolean notNull() {
    return notNull;
  }
}

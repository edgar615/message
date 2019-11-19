package com.github.edgar615.message.codegen;

import javax.lang.model.type.TypeMirror;

public class FinalBuilderField extends BuilderField {

  private Object value;

  FinalBuilderField(String name, TypeMirror typeMirror, Object value) {
    super(name, typeMirror, true);
    this.value = value;
  }

  public static FinalBuilderField notNull(String name, TypeMirror typeMirror, Object value) {
    return new FinalBuilderField(name, typeMirror, value);
  }

  public Object value() {
    return value;
  }
}

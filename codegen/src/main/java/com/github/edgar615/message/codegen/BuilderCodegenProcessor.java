package com.github.edgar615.message.codegen;

import com.github.edgar615.message.core.Event;
import com.github.edgar615.message.core.Message;
import com.google.common.base.Strings;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic.Kind;
import javax.tools.JavaFileObject;

@SupportedAnnotationTypes({"com.github.edgar615.message.codegen.BuildCodgen"})
public class BuilderCodegenProcessor extends AbstractProcessor {

  private Messager messager;
  private Elements elementUtils;
  private Filer filer;
  private Types typeUtils;

  /**
   * 被注解处理工具调用
   *
   * @param processingEnvironment 提供了Element，Filer，Messager等工具
   */
  @Override
  public synchronized void init(ProcessingEnvironment processingEnvironment) {
    super.init(processingEnvironment);
    // 返回实现Types接口的对象，用于操作类型的工具类。
    typeUtils = processingEnvironment.getTypeUtils();
    // 返回实现Elements接口的对象，用于操作元素的工具类。
    elementUtils = processingEnvironment.getElementUtils();
    // 返回实现Filer接口的对象，用于创建文件、类和辅助文件。
    filer = processingEnvironment.getFiler();
    // 返回实现Messager接口的对象，用于报告错误信息、警告提醒。
    messager = processingEnvironment.getMessager();
    // 返回指定的参数选项。
//    processingEnvironment.getOptions()
  }

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
    // for each javax.lang.model.element.Element annotated with the BuildCodgen
    if (annotations.isEmpty()) {
      return false;
    }
    Set<? extends Element> elements = roundEnv.getElementsAnnotatedWith(BuildCodgen.class);
    for (Element element : elements) {
      genSourceFile(element);
    }
    return true;
  }

  private void genSourceFile(Element element) {
    if (element.getKind() != ElementKind.CLASS) {
      processingEnv.getMessager().printMessage(
          Kind.WARNING,
          "Not a class", element);
      return;
    }
    // 类名
    String builderClassName = element.getSimpleName().toString() + "Builder";
    // 包名
    String packageName = elementUtils.getPackageOf(element).asType().toString();
    List<BuilderField> contentFields = contentFields(element);
    List<BuilderField> finalBuilderFields = finalFields(element);

    TypeSpec buildTypeSpec = TypeSpec.classBuilder(builderClassName)
        .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
        .addMethod(constructor())
        .addMethod(builderMethod(packageName, builderClassName))
        .addMethods(builderMethodSpecList(packageName, builderClassName, contentFields))
        .addFields(builderFieldSpecList(contentFields))
        .addFields(builderFieldSpecList(finalBuilderFields))
        .addField(extField())
        .addMethod(extMethod(packageName, builderClassName))
        .addMethod(buildMethod(contentFields))
        .addJavadoc("CodeGen at " + new Date())
        .addJavadoc("\n")
        .addJavadoc("@author CodeGen")
        .addJavadoc("\n")
        .build();
    JavaFile javaFile = JavaFile.builder(packageName, buildTypeSpec)
        .skipJavaLangImports(true)
        .build();

    try {
      JavaFileObject source = filer.createSourceFile(packageName + "." + builderClassName);
      processingEnv.getMessager().printMessage(Kind.NOTE,
          "Creating " + source.toUri());
      Writer writer = source.openWriter();
      writer.write(javaFile.toString());
      writer.flush();
      writer.close();
    } catch (IOException e) {
      processingEnv.getMessager().printMessage(Kind.ERROR,
          "occur error ");
      e.printStackTrace();
    }
  }

  private MethodSpec constructor() {
    return MethodSpec.constructorBuilder()
        .addModifiers(Modifier.PRIVATE)
        .build();
  }

  private MethodSpec builderMethod(String packageName, String builderClassName) {
    return MethodSpec.methodBuilder("builder")
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .returns(ClassName.get(packageName, builderClassName))
        .addStatement("return new $T()", ClassName.get(packageName, builderClassName))
        .build();
  }

  private MethodSpec extMethod(String packageName, String builderClassName) {
    return MethodSpec.methodBuilder("addExt")
        .addModifiers(Modifier.PUBLIC)
        .returns(ClassName.get(packageName, builderClassName))
        .addParameter(String.class, "key")
        .addParameter(String.class, "value")
        .addStatement("this.ext.put(key, value)")
        .addStatement("return this")
        .build();
  }

  private MethodSpec buildMethod(List<BuilderField> fields) {
    ClassName key = ClassName.get("java.lang", "String");
    ClassName value = ClassName.get("java.lang", "Object");
    ClassName map = ClassName.get("java.util", "Map");
    ClassName hashMap = ClassName.get("java.util", "HashMap");
    TypeName mapType = ParameterizedTypeName.get(map, key, value);
    MethodSpec.Builder builder = MethodSpec.methodBuilder("build")
        .addModifiers(Modifier.PUBLIC)
        .returns(Message.class);
    List<String> notNullFields = fields.stream().filter(field -> field.notNull())
        .map(field -> field.name())
        .collect(Collectors.toList());
    notNullFields.add("to");
    notNullFields.add("resource");
    for (String notNullField : notNullFields) {
      builder.addStatement("$T.requireNonNull($N, $S)", Objects.class, notNullField,
          notNullField + " can not be null");
    }
    builder.addStatement("$T content = new $T()", mapType, hashMap);
    for (BuilderField field : fields) {
      builder.addStatement("content.put($S, $N)", field.name(), field.name());
    }
    builder.addStatement("$T body = $T.create($N, $N)", Event.class, Event.class, "resource",
        "content");
    builder
        .addStatement("$T message = $T.create($N, $N)", Message.class, Message.class, "to", "body");
    builder
        .addStatement("message.header().addExts($N)", "ext");
    builder.addStatement("return $N", "message");
    return builder.build();
  }

  private FieldSpec extField() {
    ClassName ext = ClassName.get("java.lang", "String");
    ClassName map = ClassName.get("java.util", "Map");
    TypeName extType = ParameterizedTypeName.get(map, ext, ext);
    return FieldSpec.builder(extType, "ext")
        .addModifiers(Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
        .initializer("new $T<>()", HashMap.class)
        .build();
  }

  private List<BuilderField> finalFields(Element element) {
    List<BuilderField> builderFields = new ArrayList<>();
    String to = findAnnotationValue(element,
        BuildCodgen.class.getName(), "to", String.class);
    String resource = findAnnotationValue(element,
        BuildCodgen.class.getName(), "resource", String.class);
    if (Strings.isNullOrEmpty(to)) {
      builderFields.add(
          BuilderField.notNull("to", elementUtils.getTypeElement(String.class.getName()).asType()));
    } else {
      builderFields.add(FinalBuilderField
          .notNull("to", elementUtils.getTypeElement(String.class.getName()).asType(), to));
    }
    if (Strings.isNullOrEmpty(resource)) {
      builderFields.add(BuilderField
          .notNull("resource", elementUtils.getTypeElement(String.class.getName()).asType()));
    } else {
      builderFields.add(FinalBuilderField
          .notNull("resource", elementUtils.getTypeElement(String.class.getName()).asType(),
              resource));
    }
    return builderFields;
  }

  private List<BuilderField> contentFields(Element element) {
    List<BuilderField> builderFields = new ArrayList<>();
    element.getEnclosedElements().forEach(field -> {
      if (field.getKind() == ElementKind.FIELD) {
        boolean isStatic = field.getModifiers().contains(Modifier.STATIC);
        if (isStatic) {
          processingEnv.getMessager().printMessage(
              Kind.NOTE,
              "Is static", field);
          return;
        }
        String fieldName = field.getSimpleName().toString();
        if (hasAnnotation(field, NotNull.class.getName())) {
          builderFields.add(BuilderField.notNull(fieldName, field.asType()));
        } else {
          builderFields.add(BuilderField.nullable(fieldName, field.asType()));
        }

      }
    });
    builderFields
        .add(BuilderField.notNull("id", elementUtils.getTypeElement("java.lang.String").asType()));
    return builderFields;
  }

  private List<FieldSpec> builderFieldSpecList(List<BuilderField> builderFields) {
    List<FieldSpec> fieldSpecList = new ArrayList<>();
    for (BuilderField builderField : builderFields) {
      if (builderField instanceof FinalBuilderField) {
        FinalBuilderField finalBuilderField = (FinalBuilderField) builderField;
        Map<String, Object> map = new HashMap<>();
        map.put("value", finalBuilderField.value());
        String initializer = "$L";
        if (finalBuilderField.value() instanceof String) {
          initializer = "$S";
        }
        FieldSpec fieldSpec = FieldSpec
            .builder(TypeName.get(builderField.typeMirror()), builderField.name())
            .addModifiers(Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer(initializer, finalBuilderField.value())
            .build();
        fieldSpecList.add(fieldSpec);
      } else {
        FieldSpec fieldSpec = FieldSpec
            .builder(TypeName.get(builderField.typeMirror()), builderField.name())
            .addModifiers(Modifier.PRIVATE)
            .build();
        fieldSpecList.add(fieldSpec);
      }
    }
    return fieldSpecList;
  }

  private List<MethodSpec> builderMethodSpecList(String packageName, String builder,
      List<BuilderField> builderFields) {
    List<MethodSpec> methodSpecList = new ArrayList<>();
    for (BuilderField builderField : builderFields) {
      if (builderField instanceof FinalBuilderField) {
        continue;
      }
      MethodSpec methodSpec = MethodSpec.methodBuilder("set" + capitalize(builderField.name()))
          .addModifiers(Modifier.PUBLIC)
          .returns(ClassName.get(packageName, builder))
          .addParameter(TypeName.get(builderField.typeMirror()), builderField.name())
          .addStatement("this.$N = $N", builderField.name(), builderField.name())
          .addStatement("return this")
          .build();
      methodSpecList.add(methodSpec);
    }
    return methodSpecList;
  }

  private String capitalize(String name) {
    char[] c = name.toCharArray();
    c[0] = Character.toUpperCase(c[0]);
    return new String(c);
  }

  private static boolean hasAnnotation(Element element, String annotationClass) {
    for (AnnotationMirror annotationMirror : element.getAnnotationMirrors()) {
      DeclaredType annotationType = annotationMirror.getAnnotationType();
      TypeElement annotationElement = (TypeElement) annotationType
          .asElement();
      if (annotationElement.getQualifiedName().contentEquals(annotationClass)) {
        return true;
      }
    }
    return false;
  }

  private static <T> T findAnnotationValue(Element element, String annotationClass,
      String valueName, Class<T> expectedType) {
    T ret = null;
    for (AnnotationMirror annotationMirror : element.getAnnotationMirrors()) {
      DeclaredType annotationType = annotationMirror.getAnnotationType();
      TypeElement annotationElement = (TypeElement) annotationType
          .asElement();
      if (annotationElement.getQualifiedName().contentEquals(
          annotationClass)) {
        ret = extractValue(annotationMirror, valueName, expectedType);
        break;
      }
    }
    return ret;
  }

  private static <T> T extractValue(AnnotationMirror annotationMirror,
      String valueName, Class<T> expectedType) {
    Map<ExecutableElement, AnnotationValue> elementValues = new HashMap<ExecutableElement, AnnotationValue>(
        annotationMirror.getElementValues());
    for (Entry<ExecutableElement, AnnotationValue> entry : elementValues
        .entrySet()) {
      if (entry.getKey().getSimpleName().contentEquals(valueName)) {
        Object value = entry.getValue().getValue();
        return expectedType.cast(value);
      }
    }
    return null;
  }
}

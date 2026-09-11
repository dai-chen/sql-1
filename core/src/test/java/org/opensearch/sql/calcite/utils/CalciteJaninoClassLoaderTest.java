/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.junit.jupiter.api.Test;

/**
 * Guards the two reflective lookups that {@code
 * CalciteToolsHelper.OpenSearchCalcitePreparingStmt.implementEnumerable} depends on.
 *
 * <p>That method exists because Calcite's {@code EnumerableInterpretable.getBindable()} hardcodes
 * {@code EnumerableInterpretable.class.getClassLoader()} as Janino's parent classloader. The SQL
 * plugin declares {@code extendedPlugins = ['analytics-engine;optional=true']}, so when
 * analytics-engine is installed its classloader becomes the parent and — delegation being
 * parent-first — its copy of calcite-core wins. That loader cannot see {@code
 * org.opensearch.sql.*}, so every generated plan referencing our UDFs failed to compile with {@code
 * Cannot determine simple type name "org"}, breaking timechart / eventstats / top.
 *
 * <p>Both lookups below are resolved by name at runtime, so a Calcite or commons-compiler upgrade
 * that renames either one would not fail the build — it would fail in production, on the query
 * path, with an opaque reflection error. These assertions turn that into a compile-time-ish failure
 * with a message that says what to do.
 *
 * @see <a href="https://github.com/opensearch-project/sql/issues/5306">sql#5306</a>
 */
public class CalciteJaninoClassLoaderTest {

  @Test
  public void calcitePreparingStmtStillHasInternalParametersField() throws Exception {
    // implementEnumerable must write _conformance into the *same* map the DataContext reads, so it
    // reflects this private field rather than passing a copy.
    Field field =
        CalcitePrepareImpl.CalcitePreparingStmt.class.getDeclaredField("internalParameters");
    assertNotNull(
        field,
        "CalcitePreparingStmt.internalParameters is gone — implementEnumerable reflects on it to"
            + " share the parameter map with the DataContext. Re-check the field name against the"
            + " new Calcite version.");
    assertTrue(
        Map.class.isAssignableFrom(field.getType()),
        "internalParameters is no longer a Map; implementEnumerable casts it to Map<String,"
            + " Object>");
  }

  @Test
  public void commonsCompilerFactoryLookupStillResolves() throws Exception {
    // commons-compiler resolves from the parent classloader at runtime, which is why
    // compileWithPluginClassLoader goes through reflection instead of a direct call.
    ClassLoader classLoader = CalciteToolsHelper.class.getClassLoader();
    Class<?> factoryFactory =
        classLoader.loadClass("org.codehaus.commons.compiler.CompilerFactoryFactory");
    Method getDefault = factoryFactory.getMethod("getDefaultCompilerFactory", ClassLoader.class);
    assertNotNull(getDefault);

    Object factory = getDefault.invoke(null, classLoader);
    assertNotNull(factory, "no commons-compiler implementation on the classpath");

    // The three methods compileWithPluginClassLoader calls on the compiler.
    Object compiler = factory.getClass().getMethod("newSimpleCompiler").invoke(factory);
    assertNotNull(compiler.getClass().getMethod("setParentClassLoader", ClassLoader.class));
    assertNotNull(compiler.getClass().getMethod("cook", String.class));
    assertNotNull(compiler.getClass().getMethod("getClassLoader"));
  }

  @Test
  public void janinoCompilesAgainstOurClassLoader() throws Exception {
    // End-to-end shape of the fix: cook a class body that names an org.opensearch.sql type, using
    // the SQL plugin's classloader as parent. This is exactly what failed with
    // "Cannot determine simple type name \"org\"" when the parent was analytics-engine's loader.
    ClassLoader classLoader = CalciteToolsHelper.class.getClassLoader();
    Class<?> factoryFactory =
        classLoader.loadClass("org.codehaus.commons.compiler.CompilerFactoryFactory");
    Object factory =
        factoryFactory
            .getMethod("getDefaultCompilerFactory", ClassLoader.class)
            .invoke(null, classLoader);
    Object compiler = factory.getClass().getMethod("newSimpleCompiler").invoke(factory);
    compiler
        .getClass()
        .getMethod("setParentClassLoader", ClassLoader.class)
        .invoke(compiler, classLoader);

    String code =
        "public final class JaninoClassLoaderProbe {\n"
            + "  public static String probe() {\n"
            + "    return "
            + CalciteToolsHelper.class.getName()
            + ".class.getName();\n"
            + "  }\n"
            + "}\n";
    compiler.getClass().getMethod("cook", String.class).invoke(compiler, code);
    ClassLoader compiled =
        (ClassLoader) compiler.getClass().getMethod("getClassLoader").invoke(compiler);
    Class<?> probe = compiled.loadClass("JaninoClassLoaderProbe");
    Object result = probe.getMethod("probe").invoke(null);
    assertTrue(
        CalciteToolsHelper.class.getName().equals(result),
        "Janino could not resolve an org.opensearch.sql type through the plugin classloader");
  }
}

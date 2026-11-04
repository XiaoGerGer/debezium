/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipInputStream;

import net.bytebuddy.jar.asm.ClassReader;
import net.bytebuddy.jar.asm.ClassVisitor;
import net.bytebuddy.jar.asm.ClassWriter;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;

/** Adds JDBC timing to the disposable plugin copy without changing the connector's validation or SQL. */
final class ArchiveBenchmarkInstrumentation {
    private ArchiveBenchmarkInstrumentation() {
    }

    static void instrumentPlugin(Path plugin) throws Exception {
        final Path output = plugin.resolveSibling("instrumented.jar");
        final int[] replacements = { 0 };
        try (var input = new ZipInputStream(Files.newInputStream(plugin));
                var jar = new JarOutputStream(Files.newOutputStream(output))) {
            for (var entry = input.getNextEntry(); entry != null; entry = input.getNextEntry()) {
                jar.putNextEntry(new JarEntry(entry.getName()));
                final byte[] bytes = input.readAllBytes();
                if (entry.getName().equals("io/debezium/connector/dameng/DamengConnection.class")) {
                    final var writer = new ClassWriter(0);
                    new ClassReader(bytes).accept(new ClassVisitor(Opcodes.ASM9, writer) {
                        @Override
                        public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
                            final var original = super.visitMethod(access, name, descriptor, signature, exceptions);
                            if (!name.equals("connectionFactory")) {
                                return original;
                            }
                            return new MethodVisitor(Opcodes.ASM9, original) {
                                @Override
                                public void visitMethodInsn(int opcode, String owner, String method, String signature, boolean isInterface) {
                                    if (owner.equals("io/debezium/jdbc/JdbcConnection") && method.equals("patternBasedFactory")) {
                                        replacements[0]++;
                                        super.visitMethodInsn(opcode, "io/debezium/connector/dameng/ArchiveBenchmarkConnectionFactory", method, signature, isInterface);
                                    }
                                    else {
                                        super.visitMethodInsn(opcode, owner, method, signature, isInterface);
                                    }
                                }
                            };
                        }
                    }, 0);
                    jar.write(writer.toByteArray());
                }
                else {
                    jar.write(bytes);
                }
                jar.closeEntry();
            }
        }
        if (replacements[0] != 1) {
            throw new IllegalStateException("Expected exactly one JDBC factory instrumentation, found " + replacements[0]);
        }
        Files.move(output, plugin, StandardCopyOption.REPLACE_EXISTING);
    }
}

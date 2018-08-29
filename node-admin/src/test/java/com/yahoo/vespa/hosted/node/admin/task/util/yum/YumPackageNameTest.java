// Copyright 2018 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.node.admin.task.util.yum;

import org.junit.Test;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.containsStringIgnoringCase;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class YumPackageNameTest {

    @Test
    public void testAllValidFormats() {
        // name
        verifyPackageName(
                "docker-engine-selinux",
                null,
                "docker-engine-selinux",
                null,
                null,
                null,
                "docker-engine-selinux",
                null);

        // name.arch
        verifyPackageName(
                "docker-engine-selinux.x86_64",
                null,
                "docker-engine-selinux",
                null,
                null,
                "x86_64",
                "docker-engine-selinux.x86_64",
                null);

        // name-ver-rel
        verifyPackageName("docker-engine-selinux-1.12.6-1.el7",
                null,
                "docker-engine-selinux",
                "1.12.6",
                "1.el7",
                null,
                "docker-engine-selinux-1.12.6-1.el7",
                null);

        // name-ver-rel.arch
        verifyPackageName("docker-engine-selinux-1.12.6-1.el7.x86_64",
                null,
                "docker-engine-selinux",
                "1.12.6",
                "1.el7",
                "x86_64",
                "docker-engine-selinux-1.12.6-1.el7.x86_64",
                null);

        // name-epoch:ver-rel.arch
        verifyPackageName(
                "docker-2:1.12.6-71.git3e8e77d.el7.centos.1.x86_64",
                "2",
                "docker",
                "1.12.6",
                "71.git3e8e77d.el7.centos.1",
                "x86_64",
                "2:docker-1.12.6-71.git3e8e77d.el7.centos.1.x86_64",
                "2:docker-1.12.6-71.git3e8e77d.el7.centos.1.*");

        // epoch:name-ver-rel.arch
        verifyPackageName(
                "2:docker-1.12.6-71.git3e8e77d.el7.centos.1.x86_64",
                "2",
                "docker",
                "1.12.6",
                "71.git3e8e77d.el7.centos.1",
                "x86_64",
                "2:docker-1.12.6-71.git3e8e77d.el7.centos.1.x86_64",
                "2:docker-1.12.6-71.git3e8e77d.el7.centos.1.*");
    }

    private void verifyPackageName(String packageName,
                                   String epoch,
                                   String name,
                                   String version,
                                   String release,
                                   String architecture,
                                   String toName,
                                   String toVersionName) {
        YumPackageName yumPackageName = YumPackageName.fromString(packageName);
        verifyValue(epoch, yumPackageName.getEpoch());
        verifyValue(name, Optional.of(yumPackageName.getName()));
        verifyValue(version, yumPackageName.getVersion());
        verifyValue(release, yumPackageName.getRelease());
        verifyValue(architecture, yumPackageName.getArchitecture());
        verifyValue(toName, Optional.of(yumPackageName.toName()));

        if (toVersionName == null) {
            try {
                yumPackageName.toVersionLockName();
                fail();
            } catch (IllegalStateException e) {
                assertThat(e.getMessage(), containsStringIgnoringCase("epoch is missing"));
            }
        } else {
            assertEquals(toVersionName, yumPackageName.toVersionLockName());
        }
    }

    private void verifyValue(String value, Optional<String> actual) {
        if (value == null) {
            assertFalse(actual.isPresent());
        } else {
            assertEquals(value, actual.get());
        }
    }

    @Test
    public void testArchitectures() {
        assertEquals("x86_64", YumPackageName.fromString("docker.x86_64").getArchitecture().get());
        assertEquals("i686", YumPackageName.fromString("docker.i686").getArchitecture().get());
        assertEquals("noarch", YumPackageName.fromString("docker.noarch").getArchitecture().get());
    }

    @Test
    public void unrecognizedArchitectureGetsGobbledUp() {
        YumPackageName packageName = YumPackageName.fromString("docker-engine-selinux-1.12.6-1.el7.i486");
        // This is not a great feature - please use YumPackageName.Builder instead.
        assertEquals("1.el7.i486", packageName.getRelease().get());
    }

    @Test
    public void failParsingOfPackageNameWithEpochAndArchitecture() {
        try {
            YumPackageName.fromString("epoch:docker-engine-selinux-1.12.6-1.el7.x86_64");
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsStringIgnoringCase("epoch"));
        }
    }
}
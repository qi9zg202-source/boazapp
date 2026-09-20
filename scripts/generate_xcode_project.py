#!/usr/bin/env python3
"""Regenerate the Xcode project from checked-in Swift sources and pinned packages."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "boazapp.xcodeproj" / "project.pbxproj"
GRDB_URL = "https://github.com/groue/GRDB.swift.git"
GRDB_VERSION = "7.11.1"


def selected_development_team() -> str | None:
    """Keep a locally selected Xcode team when source changes regenerate the project."""
    if not PROJECT.exists():
        return None
    teams = set(re.findall(r'\bDEVELOPMENT_TEAM\s*=\s*"?([A-Z0-9]{10})"?\s*;', PROJECT.read_text()))
    if len(teams) > 1:
        raise ValueError("Conflicting development teams in the Xcode project; resolve signing before regenerating")
    return next(iter(teams), None)


def key(name: str) -> str:
    return hashlib.sha1(name.encode()).hexdigest()[:24].upper()


def value(text: str) -> str:
    return json.dumps(text, ensure_ascii=False)


def obj(name: str, fields: list[str]) -> str:
    return f"\t\t{key(name)} = {{\n" + "\n".join(f"\t\t\t{field}" for field in fields) + "\n\t\t};"


def list_field(name: str, values: list[str]) -> str:
    return f"{name} = ( " + " ".join(f"{item}," for item in values) + " );"


def config(name: str, settings: dict[str, str]) -> str:
    settings_text = " ".join(f"{item} = {setting};" for item, setting in settings.items())
    return obj(name, ["isa = XCBuildConfiguration;", f"buildSettings = {{ {settings_text} }};", f"name = {name.split(':')[-1]};"])


def generated_project() -> str:
    development_team = selected_development_team()
    sources = sorted(path.relative_to(ROOT / "boazapp").as_posix() for path in (ROOT / "boazapp").rglob("*.swift"))
    tests = sorted(path.relative_to(ROOT / "Tests").as_posix() for path in (ROOT / "Tests").rglob("*.swift"))
    test_resources = sorted(path.relative_to(ROOT / "Tests").as_posix() for path in (ROOT / "Tests" / "Fixtures").rglob("*.sql"))
    test_harnesses = ["../scripts/GatewayTransportHarness.swift", "../scripts/LocalCoreHarness.swift"]
    test_sources = tests + test_harnesses
    resources = ["Core/Database/Schema.sql"]
    if (ROOT / "boazapp" / "Resources" / "Assets.xcassets").exists():
        resources.append("Resources/Assets.xcassets")
    refs: list[str] = []
    builds: list[str] = []
    for path in sources + resources + ["Resources/Info.plist", "Resources/boazapp.entitlements"]:
        kind = "sourcecode.swift" if path.endswith(".swift") else "text.plist.xml" if path.endswith(".plist") else "text.plist.entitlements" if path.endswith(".entitlements") else "folder.assetcatalog" if path.endswith(".xcassets") else "text"
        refs.append(obj(f"app-file:{path}", ["isa = PBXFileReference;", f"lastKnownFileType = {kind};", f"path = {value(path)};", 'sourceTree = "<group>";']))
        if path in sources + resources:
            builds.append(obj(f"app-build:{path}", ["isa = PBXBuildFile;", f"fileRef = {key(f'app-file:{path}')};"]))
    for path in test_sources:
        refs.append(obj(f"test-file:{path}", ["isa = PBXFileReference;", "lastKnownFileType = sourcecode.swift;", f"path = {value(path)};", 'sourceTree = "<group>";']))
        builds.append(obj(f"test-build:{path}", ["isa = PBXBuildFile;", f"fileRef = {key(f'test-file:{path}')};"]))
    for path in test_resources:
        refs.append(obj(f"test-file:{path}", ["isa = PBXFileReference;", "lastKnownFileType = text;", f"path = {value(path)};", 'sourceTree = "<group>";']))
        builds.append(obj(f"test-build:{path}", ["isa = PBXBuildFile;", f"fileRef = {key(f'test-file:{path}')};"]))
    refs.extend([
        obj("product:app", ["isa = PBXFileReference;", "explicitFileType = wrapper.application;", "includeInIndex = 0;", "path = boazapp.app;", "sourceTree = BUILT_PRODUCTS_DIR;"]),
        obj("product:tests", ["isa = PBXFileReference;", "explicitFileType = wrapper.cfbundle;", "includeInIndex = 0;", "path = boazappTests.xctest;", "sourceTree = BUILT_PRODUCTS_DIR;"]),
    ])
    groups = [
        obj("group:root", ["isa = PBXGroup;", list_field("children", [key("group:app"), key("group:tests"), key("group:products")]), 'sourceTree = "<group>";']),
        obj("group:app", ["isa = PBXGroup;", list_field("children", [key(f"app-file:{path}") for path in sources + resources + ["Resources/Info.plist", "Resources/boazapp.entitlements"]]), "path = boazapp;", 'sourceTree = "<group>";']),
        obj("group:tests", ["isa = PBXGroup;", list_field("children", [key(f"test-file:{path}") for path in test_sources + test_resources]), "path = Tests;", 'sourceTree = "<group>";']),
        obj("group:products", ["isa = PBXGroup;", list_field("children", [key("product:app"), key("product:tests")]), "name = Products;", 'sourceTree = "<group>";']),
    ]
    phases = [
        obj("phase:app-sources", ["isa = PBXSourcesBuildPhase;", "buildActionMask = 2147483647;", list_field("files", [key(f"app-build:{path}") for path in sources]), "runOnlyForDeploymentPostprocessing = 0;"]),
        obj("phase:app-resources", ["isa = PBXResourcesBuildPhase;", "buildActionMask = 2147483647;", list_field("files", [key(f"app-build:{path}") for path in resources]), "runOnlyForDeploymentPostprocessing = 0;"]),
        obj("phase:test-sources", ["isa = PBXSourcesBuildPhase;", "buildActionMask = 2147483647;", list_field("files", [key(f"test-build:{path}") for path in test_sources]), "runOnlyForDeploymentPostprocessing = 0;"]),
        obj("phase:test-resources", ["isa = PBXResourcesBuildPhase;", "buildActionMask = 2147483647;", list_field("files", [key(f"test-build:{path}") for path in test_resources]), "runOnlyForDeploymentPostprocessing = 0;"]),
        obj("phase:app-frameworks", ["isa = PBXFrameworksBuildPhase;", "buildActionMask = 2147483647;", list_field("files", [key("app-build:GRDB")]), "runOnlyForDeploymentPostprocessing = 0;"]),
        obj("phase:test-frameworks", ["isa = PBXFrameworksBuildPhase;", "buildActionMask = 2147483647;", list_field("files", [key("test-build:GRDB")]), "runOnlyForDeploymentPostprocessing = 0;"]),
    ]
    target_app = obj("target:app", [
        "isa = PBXNativeTarget;", f"buildConfigurationList = {key('list:app')};",
        list_field("buildPhases", [key("phase:app-sources"), key("phase:app-frameworks"), key("phase:app-resources")]),
        "buildRules = ();", "dependencies = ();", "name = boazapp;", "productName = boazapp;",
        list_field("packageProductDependencies", [key("package-product:app:GRDB")]),
        f"productReference = {key('product:app')};", 'productType = "com.apple.product-type.application";',
    ])
    target_tests = obj("target:tests", [
        "isa = PBXNativeTarget;", f"buildConfigurationList = {key('list:tests')};",
        list_field("buildPhases", [key("phase:test-sources"), key("phase:test-frameworks"), key("phase:test-resources")]),
        "buildRules = ();", list_field("dependencies", [key("dependency:test-app")]),
        list_field("packageProductDependencies", [key("package-product:test:GRDB")]),
        "name = boazappTests;", "productName = boazappTests;", f"productReference = {key('product:tests')};",
        'productType = "com.apple.product-type.bundle.unit-test";',
    ])
    project = obj("project", [
        "isa = PBXProject;",
        f"attributes = {{ BuildIndependentTargetsInParallel = 1; LastUpgradeCheck = 2700; TargetAttributes = {{ {key('target:app')} = {{ CreatedOnToolsVersion = 27.0; SystemCapabilities = {{ com.apple.HealthKit = {{ enabled = 1; }}; }}; }}; }}; }};",
        f"buildConfigurationList = {key('list:project')};", 'compatibilityVersion = "Xcode 14.0";',
        "developmentRegion = en;", "hasScannedForEncodings = 0;", "knownRegions = ( en, Base, );",
        f"mainGroup = {key('group:root')};", f"productRefGroup = {key('group:products')};",
        list_field("packageReferences", [key("package:GRDB")]),
        'projectDirPath = "";', 'projectRoot = "";', list_field("targets", [key("target:app"), key("target:tests")]),
    ])
    common = {
        "SDKROOT": "iphoneos", "IPHONEOS_DEPLOYMENT_TARGET": "17.0", "SWIFT_VERSION": "6.0",
        "SWIFT_STRICT_CONCURRENCY": "complete", "CLANG_ENABLE_MODULES": "YES", "SWIFT_EMIT_LOC_STRINGS": "YES",
    }
    app = {
        "PRODUCT_BUNDLE_IDENTIFIER": "com.beckliu.boazhealth", "PRODUCT_NAME": "boazapp",
        "INFOPLIST_FILE": value("boazapp/Resources/Info.plist"), "GENERATE_INFOPLIST_FILE": "NO",
        "CODE_SIGN_ENTITLEMENTS": value("boazapp/Resources/boazapp.entitlements"),
        "CODE_SIGN_STYLE": "Automatic", "CURRENT_PROJECT_VERSION": "1",
        "MARKETING_VERSION": "0.1.0", "TARGETED_DEVICE_FAMILY": value("1"),
        "SUPPORTS_MACCATALYST": "NO", "ENABLE_PREVIEWS": "YES",
        "ASSETCATALOG_COMPILER_APPICON_NAME": "AppIcon",
    }
    test = {
        "PRODUCT_BUNDLE_IDENTIFIER": "com.beckliu.boazhealth.tests", "PRODUCT_NAME": "boazappTests",
        "GENERATE_INFOPLIST_FILE": "YES", "CODE_SIGN_STYLE": "Automatic",
        "TEST_HOST": value("$(BUILT_PRODUCTS_DIR)/boazapp.app/boazapp"), "BUNDLE_LOADER": value("$(TEST_HOST)"),
        "OTHER_LDFLAGS": value("-lsqlite3"),
    }
    if development_team:
        app["DEVELOPMENT_TEAM"] = development_team
        test["DEVELOPMENT_TEAM"] = development_team
    configs = []
    for target, settings in [("project", common), ("app", common | app), ("tests", common | test)]:
        for flavor in ["Debug", "Release"]:
            extra = {"SWIFT_OPTIMIZATION_LEVEL": value("-Onone") if flavor == "Debug" else value("-O")}
            if flavor == "Debug":
                extra["SWIFT_ACTIVE_COMPILATION_CONDITIONS"] = value("$(inherited) DEBUG")
            if target == "app" and flavor == "Debug":
                extra["ENABLE_TESTABILITY"] = "YES"
            configs.append(config(f"config:{target}:{flavor}", settings | extra))
    lists = [obj(f"list:{target}", ["isa = XCConfigurationList;", list_field("buildConfigurations", [key(f"config:{target}:Debug"), key(f"config:{target}:Release")]), "defaultConfigurationIsVisible = 0;", "defaultConfigurationName = Release;"]) for target in ["project", "app", "tests"]]
    dependency = obj("dependency:test-app", ["isa = PBXTargetDependency;", f"target = {key('target:app')};", f"targetProxy = {key('proxy:test-app')};"])
    proxy = obj("proxy:test-app", ["isa = PBXContainerItemProxy;", f"containerPortal = {key('project')};", "proxyType = 1;", f"remoteGlobalIDString = {key('target:app')};", "remoteInfo = boazapp;"])
    package = obj("package:GRDB", [
        "isa = XCRemoteSwiftPackageReference;",
        f"repositoryURL = {value(GRDB_URL)};",
        f"requirement = {{ kind = exactVersion; version = {value(GRDB_VERSION)}; }};",
    ])
    products = [
        obj(f"package-product:{target}:GRDB", [
            "isa = XCSwiftPackageProductDependency;",
            f"package = {key('package:GRDB')};",
            "productName = GRDB;",
        ])
        for target in ["app", "test"]
    ]
    package_builds = [
        obj(f"{target}-build:GRDB", ["isa = PBXBuildFile;", f"productRef = {key(f'package-product:{target}:GRDB')};"])
        for target in ["app", "test"]
    ]
    objects = refs + builds + package_builds + groups + phases + [target_app, target_tests, project, dependency, proxy, package] + products + configs + lists
    return "// !$*UTF8*$!\n{\n\tarchiveVersion = 1;\n\tclasses = {};\n\tobjectVersion = 56;\n\tobjects = {\n" + "\n".join(objects) + f"\n\t}};\n\trootObject = {key('project')};\n}}\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail if the checked-in project is not current")
    arguments = parser.parse_args()
    rendered = generated_project()
    if arguments.check:
        current = PROJECT.read_text() if PROJECT.exists() else ""
        if current != rendered:
            sys.stderr.writelines(difflib.unified_diff(
                current.splitlines(keepends=True), rendered.splitlines(keepends=True),
                fromfile=str(PROJECT), tofile="generated project",
            ))
            raise SystemExit("Xcode project is stale; run scripts/generate_xcode_project.py")
        print(f"Xcode project is current: {PROJECT}")
        return
    PROJECT.parent.mkdir(parents=True, exist_ok=True)
    PROJECT.write_text(rendered)


if __name__ == "__main__":
    main()

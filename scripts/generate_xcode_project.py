#!/usr/bin/env python3
"""Regenerate the dependency-free Xcode project from the checked-in Swift sources."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "boazapp.xcodeproj" / "project.pbxproj"


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


def main() -> None:
    sources = sorted(path.relative_to(ROOT / "boazapp").as_posix() for path in (ROOT / "boazapp").rglob("*.swift"))
    tests = sorted(path.relative_to(ROOT / "Tests").as_posix() for path in (ROOT / "Tests").rglob("*.swift"))
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
    for path in tests:
        refs.append(obj(f"test-file:{path}", ["isa = PBXFileReference;", "lastKnownFileType = sourcecode.swift;", f"path = {value(path)};", 'sourceTree = "<group>";']))
        builds.append(obj(f"test-build:{path}", ["isa = PBXBuildFile;", f"fileRef = {key(f'test-file:{path}')};"]))
    refs.extend([
        obj("product:app", ["isa = PBXFileReference;", "explicitFileType = wrapper.application;", "includeInIndex = 0;", "path = boazapp.app;", "sourceTree = BUILT_PRODUCTS_DIR;"]),
        obj("product:tests", ["isa = PBXFileReference;", "explicitFileType = wrapper.cfbundle;", "includeInIndex = 0;", "path = boazappTests.xctest;", "sourceTree = BUILT_PRODUCTS_DIR;"]),
    ])
    groups = [
        obj("group:root", ["isa = PBXGroup;", list_field("children", [key("group:app"), key("group:tests"), key("group:products")]), 'sourceTree = "<group>";']),
        obj("group:app", ["isa = PBXGroup;", list_field("children", [key(f"app-file:{path}") for path in sources + resources + ["Resources/Info.plist", "Resources/boazapp.entitlements"]]), "path = boazapp;", 'sourceTree = "<group>";']),
        obj("group:tests", ["isa = PBXGroup;", list_field("children", [key(f"test-file:{path}") for path in tests]), "path = Tests;", 'sourceTree = "<group>";']),
        obj("group:products", ["isa = PBXGroup;", list_field("children", [key("product:app"), key("product:tests")]), "name = Products;", 'sourceTree = "<group>";']),
    ]
    phases = [
        obj("phase:app-sources", ["isa = PBXSourcesBuildPhase;", "buildActionMask = 2147483647;", list_field("files", [key(f"app-build:{path}") for path in sources]), "runOnlyForDeploymentPostprocessing = 0;"]),
        obj("phase:app-resources", ["isa = PBXResourcesBuildPhase;", "buildActionMask = 2147483647;", list_field("files", [key(f"app-build:{path}") for path in resources]), "runOnlyForDeploymentPostprocessing = 0;"]),
        obj("phase:test-sources", ["isa = PBXSourcesBuildPhase;", "buildActionMask = 2147483647;", list_field("files", [key(f"test-build:{path}") for path in tests]), "runOnlyForDeploymentPostprocessing = 0;"]),
        obj("phase:app-frameworks", ["isa = PBXFrameworksBuildPhase;", "buildActionMask = 2147483647;", "files = ();", "runOnlyForDeploymentPostprocessing = 0;"]),
        obj("phase:test-frameworks", ["isa = PBXFrameworksBuildPhase;", "buildActionMask = 2147483647;", "files = ();", "runOnlyForDeploymentPostprocessing = 0;"]),
    ]
    target_app = obj("target:app", [
        "isa = PBXNativeTarget;", f"buildConfigurationList = {key('list:app')};",
        list_field("buildPhases", [key("phase:app-sources"), key("phase:app-frameworks"), key("phase:app-resources")]),
        "buildRules = ();", "dependencies = ();", "name = boazapp;", "productName = boazapp;",
        f"productReference = {key('product:app')};", 'productType = "com.apple.product-type.application";',
    ])
    target_tests = obj("target:tests", [
        "isa = PBXNativeTarget;", f"buildConfigurationList = {key('list:tests')};",
        list_field("buildPhases", [key("phase:test-sources"), key("phase:test-frameworks")]),
        "buildRules = ();", list_field("dependencies", [key("dependency:test-app")]),
        "name = boazappTests;", "productName = boazappTests;", f"productReference = {key('product:tests')};",
        'productType = "com.apple.product-type.bundle.unit-test";',
    ])
    project = obj("project", [
        "isa = PBXProject;",
        f"attributes = {{ BuildIndependentTargetsInParallel = 1; LastUpgradeCheck = 2700; TargetAttributes = {{ {key('target:app')} = {{ CreatedOnToolsVersion = 27.0; SystemCapabilities = {{ com.apple.HealthKit = {{ enabled = 1; }}; }}; }}; }}; }};",
        f"buildConfigurationList = {key('list:project')};", 'compatibilityVersion = "Xcode 14.0";',
        "developmentRegion = en;", "hasScannedForEncodings = 0;", "knownRegions = ( en, Base, );",
        f"mainGroup = {key('group:root')};", f"productRefGroup = {key('group:products')};",
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
        "OTHER_LDFLAGS": value("-lsqlite3"), "SUPPORTS_MACCATALYST": "NO", "ENABLE_PREVIEWS": "YES",
        "ASSETCATALOG_COMPILER_APPICON_NAME": "AppIcon",
    }
    test = {
        "PRODUCT_BUNDLE_IDENTIFIER": "com.beckliu.boazhealth.tests", "PRODUCT_NAME": "boazappTests",
        "GENERATE_INFOPLIST_FILE": "YES", "CODE_SIGN_STYLE": "Automatic",
        "TEST_HOST": value("$(BUILT_PRODUCTS_DIR)/boazapp.app/boazapp"), "BUNDLE_LOADER": value("$(TEST_HOST)"),
    }
    configs = []
    for target, settings in [("project", common), ("app", common | app), ("tests", common | test)]:
        for flavor in ["Debug", "Release"]:
            extra = {"SWIFT_OPTIMIZATION_LEVEL": value("-Onone") if flavor == "Debug" else value("-O")}
            if target == "app" and flavor == "Debug":
                extra["ENABLE_TESTABILITY"] = "YES"
            configs.append(config(f"config:{target}:{flavor}", settings | extra))
    lists = [obj(f"list:{target}", ["isa = XCConfigurationList;", list_field("buildConfigurations", [key(f"config:{target}:Debug"), key(f"config:{target}:Release")]), "defaultConfigurationIsVisible = 0;", "defaultConfigurationName = Release;"]) for target in ["project", "app", "tests"]]
    dependency = obj("dependency:test-app", ["isa = PBXTargetDependency;", f"target = {key('target:app')};", f"targetProxy = {key('proxy:test-app')};"])
    proxy = obj("proxy:test-app", ["isa = PBXContainerItemProxy;", f"containerPortal = {key('project')};", "proxyType = 1;", f"remoteGlobalIDString = {key('target:app')};", "remoteInfo = boazapp;"])
    objects = refs + builds + groups + phases + [target_app, target_tests, project, dependency, proxy] + configs + lists
    PROJECT.parent.mkdir(parents=True, exist_ok=True)
    PROJECT.write_text("// !$*UTF8*$!\n{\n\tarchiveVersion = 1;\n\tclasses = {};\n\tobjectVersion = 56;\n\tobjects = {\n" + "\n".join(objects) + f"\n\t}};\n\trootObject = {key('project')};\n}}\n")


if __name__ == "__main__":
    main()

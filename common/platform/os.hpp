/**
 * Copyright 2026 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef TINYLAMB_COMMON_PLATFORM_OS_HPP
#define TINYLAMB_COMMON_PLATFORM_OS_HPP

// Target-OS detection. Prefer these over raw compiler macros: in particular
// Apple's compilers do not define __unix__, so `#if defined(__unix__)`
// silently excludes macOS from POSIX-only code paths.
#ifdef __linux__
#define TINYLAMB_OS_LINUX 1
#elif defined(__APPLE__) && defined(__MACH__)
#define TINYLAMB_OS_MACOS 1
#elif defined(_WIN32)
#define TINYLAMB_OS_WINDOWS 1
#endif

#if defined(__unix__) || defined(__APPLE__)
#define TINYLAMB_OS_POSIX 1
#endif

#endif  // TINYLAMB_COMMON_PLATFORM_OS_HPP

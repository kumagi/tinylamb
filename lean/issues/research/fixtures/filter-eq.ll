; ModuleID = 'tinylamb_filter'
source_filename = "tinylamb_filter"
target datalayout = "e-m:e-p270:32:32-p271:32:32-p272:64:64-i64:64-i128:128-f80:128-n8:16:32:64-S128"
target triple = "x86_64-pc-linux-gnu"

; Function Attrs: nofree norecurse nosync nounwind memory(argmem: readwrite)
define void @tinylamb_filter(ptr noalias readonly captures(none) %0, ptr noalias writeonly captures(none) %1, i64 %2, i64 %3) local_unnamed_addr #0 {
entry:
  %.not = icmp eq i64 %2, 0
  br i1 %.not, label %exit, label %iter.check

iter.check:                                       ; preds = %entry
  %min.iters.check = icmp ult i64 %2, 8
  br i1 %min.iters.check, label %body.preheader, label %vector.main.loop.iter.check

vector.main.loop.iter.check:                      ; preds = %iter.check
  %min.iters.check2 = icmp ult i64 %2, 32
  br i1 %min.iters.check2, label %vec.epilog.ph, label %vector.ph

vector.ph:                                        ; preds = %vector.main.loop.iter.check
  %n.vec = and i64 %2, -32
  %broadcast.splatinsert = insertelement <8 x i64> poison, i64 %3, i64 0
  %broadcast.splat = shufflevector <8 x i64> %broadcast.splatinsert, <8 x i64> poison, <8 x i32> zeroinitializer
  br label %vector.body

vector.body:                                      ; preds = %vector.body, %vector.ph
  %index = phi i64 [ 0, %vector.ph ], [ %index.next, %vector.body ]
  %4 = getelementptr i64, ptr %0, i64 %index
  %5 = getelementptr i8, ptr %4, i64 64
  %6 = getelementptr i8, ptr %4, i64 128
  %7 = getelementptr i8, ptr %4, i64 192
  %wide.load = load <8 x i64>, ptr %4, align 4
  %wide.load3 = load <8 x i64>, ptr %5, align 4
  %wide.load4 = load <8 x i64>, ptr %6, align 4
  %wide.load5 = load <8 x i64>, ptr %7, align 4
  %8 = icmp eq <8 x i64> %wide.load, %broadcast.splat
  %9 = icmp eq <8 x i64> %wide.load3, %broadcast.splat
  %10 = icmp eq <8 x i64> %wide.load4, %broadcast.splat
  %11 = icmp eq <8 x i64> %wide.load5, %broadcast.splat
  %12 = getelementptr i8, ptr %1, i64 %index
  %13 = zext <8 x i1> %8 to <8 x i8>
  %14 = zext <8 x i1> %9 to <8 x i8>
  %15 = zext <8 x i1> %10 to <8 x i8>
  %16 = zext <8 x i1> %11 to <8 x i8>
  %17 = getelementptr i8, ptr %12, i64 8
  %18 = getelementptr i8, ptr %12, i64 16
  %19 = getelementptr i8, ptr %12, i64 24
  store <8 x i8> %13, ptr %12, align 1
  store <8 x i8> %14, ptr %17, align 1
  store <8 x i8> %15, ptr %18, align 1
  store <8 x i8> %16, ptr %19, align 1
  %index.next = add nuw i64 %index, 32
  %20 = icmp eq i64 %index.next, %n.vec
  br i1 %20, label %middle.block, label %vector.body, !llvm.loop !0

middle.block:                                     ; preds = %vector.body
  %cmp.n = icmp eq i64 %2, %n.vec
  br i1 %cmp.n, label %exit, label %vec.epilog.iter.check

vec.epilog.iter.check:                            ; preds = %middle.block
  %n.vec.remaining = and i64 %2, 24
  %min.epilog.iters.check = icmp eq i64 %n.vec.remaining, 0
  br i1 %min.epilog.iters.check, label %body.preheader, label %vec.epilog.ph

vec.epilog.ph:                                    ; preds = %vec.epilog.iter.check, %vector.main.loop.iter.check
  %vec.epilog.resume.val = phi i64 [ %n.vec, %vec.epilog.iter.check ], [ 0, %vector.main.loop.iter.check ]
  %n.vec7 = and i64 %2, -8
  %broadcast.splatinsert8 = insertelement <8 x i64> poison, i64 %3, i64 0
  %broadcast.splat9 = shufflevector <8 x i64> %broadcast.splatinsert8, <8 x i64> poison, <8 x i32> zeroinitializer
  br label %vec.epilog.vector.body

vec.epilog.vector.body:                           ; preds = %vec.epilog.vector.body, %vec.epilog.ph
  %index10 = phi i64 [ %vec.epilog.resume.val, %vec.epilog.ph ], [ %index.next12, %vec.epilog.vector.body ]
  %21 = getelementptr i64, ptr %0, i64 %index10
  %wide.load11 = load <8 x i64>, ptr %21, align 4
  %22 = icmp eq <8 x i64> %wide.load11, %broadcast.splat9
  %23 = getelementptr i8, ptr %1, i64 %index10
  %24 = zext <8 x i1> %22 to <8 x i8>
  store <8 x i8> %24, ptr %23, align 1
  %index.next12 = add nuw i64 %index10, 8
  %25 = icmp eq i64 %index.next12, %n.vec7
  br i1 %25, label %vec.epilog.middle.block, label %vec.epilog.vector.body, !llvm.loop !3

vec.epilog.middle.block:                          ; preds = %vec.epilog.vector.body
  %cmp.n13 = icmp eq i64 %2, %n.vec7
  br i1 %cmp.n13, label %exit, label %body.preheader

body.preheader:                                   ; preds = %vec.epilog.iter.check, %vec.epilog.middle.block, %iter.check
  %index1.ph = phi i64 [ 0, %iter.check ], [ %n.vec, %vec.epilog.iter.check ], [ %n.vec7, %vec.epilog.middle.block ]
  br label %body

body:                                             ; preds = %body.preheader, %body
  %index1 = phi i64 [ %31, %body ], [ %index1.ph, %body.preheader ]
  %26 = getelementptr i64, ptr %0, i64 %index1
  %27 = load i64, ptr %26, align 4
  %28 = icmp eq i64 %27, %3
  %29 = getelementptr i8, ptr %1, i64 %index1
  %30 = zext i1 %28 to i8
  store i8 %30, ptr %29, align 1
  %31 = add nuw i64 %index1, 1
  %exitcond.not = icmp eq i64 %31, %2
  br i1 %exitcond.not, label %exit, label %body, !llvm.loop !4

exit:                                             ; preds = %body, %middle.block, %vec.epilog.middle.block, %entry
  ret void
}

attributes #0 = { nofree norecurse nosync nounwind memory(argmem: readwrite) "target-cpu"="znver5" "target-features"="+prfchw,-cldemote,+avx,+aes,+sahf,+pclmul,-xop,+crc32,-amx-fp8,+xsaves,-avx512fp16,-usermsr,-sm4,-egpr,+sse4.1,+avx512ifma,+xsave,+sse4.2,-tsxldtrk,-sm3,-ptwrite,-widekl,-movrs,+invpcid,+64bit,+xsavec,-avx10.1-512,+avx512vpopcntdq,+cmov,+avx512vp2intersect,+avx512cd,+movbe,-avxvnniint8,-ccmp,-amx-int8,-kl,-avx10.1-256,+evex512,+avxvnni,-rtm,+adx,+avx2,-hreset,+movdiri,-serialize,-sha512,+vpclmulqdq,+avx512vl,-uintr,-cf,+clflushopt,-raoint,-cmpccxadd,+bmi,-amx-tile,+sse,-avx10.2-256,+gfni,-avxvnniint16,-amx-fp16,-zu,-ndd,+xsaveopt,+rdrnd,+avx512f,-amx-bf16,+avx512bf16,+avx512vnni,-push2pop2,+cx8,+avx512bw,+sse3,+pku,-nf,-amx-tf32,-amx-avx512,+fsgsbase,+clzero,+mwaitx,-lwp,+lzcnt,+sha,+movdir64b,-ppx,+wbnoinvd,-enqcmd,-amx-transpose,-avx10.2-512,-avxneconvert,-tbm,-pconfig,-amx-complex,+ssse3,+cx16,+bmi2,+fma,+popcnt,-avxifma,+f16c,+avx512bitalg,+rdpru,+clwb,+mmx,+sse2,+rdseed,+avx512vbmi2,-prefetchi,-amx-movrs,+rdpid,-fma4,+avx512vbmi,+shstk,+vaes,-waitpkg,-sgx,+fxsr,+avx512dq,+sse4a" }

!0 = distinct !{!0, !1, !2}
!1 = !{!"llvm.loop.isvectorized", i32 1}
!2 = !{!"llvm.loop.unroll.runtime.disable"}
!3 = distinct !{!3, !1, !2}
!4 = distinct !{!4, !2, !1}

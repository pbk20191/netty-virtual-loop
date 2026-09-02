package io.github.pbk20191.virtualloop

import sun.misc.Unsafe
import java.lang.invoke.LambdaConversionException
import java.lang.invoke.LambdaMetafactory
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType
import java.util.function.Function

object LambdaMetafactoryDemo {
    // ---- 1. 논캡처: () -> Runnable ----
    @JvmStatic
    fun impl1() {
        println("  [1] hello from generated Runnable")
    }

    // ---- 2. 캡처: (String) -> Runnable ----
    @JvmStatic
    fun impl2(captured: String?) {
        println("  [2] captured = " + captured)
    }

    // ---- 3. 제네릭 특수화: Function<String,Integer> ----
    @JvmStatic
    fun impl3(s: String): Int {
        return s.length
    }



    @Throws(Throwable::class)
    @JvmStatic
    fun main(args: Array<String>) {


        val lk = MethodHandles.lookup()
        java.lang.reflect.Proxy::newProxyInstance

        // 1
        val m1 = lk.findStatic(LambdaMetafactoryDemo::class.java, LambdaMetafactoryDemo::impl1.name, MethodType.methodType(Void.TYPE))
        val r1 = LambdaMetafactory.metafactory(
            lk, Runnable::run.name,
            MethodType.methodType(Runnable::class.java),  // factoryType: 캡처 없음 → Runnable 반환
            MethodType.methodType(Void.TYPE),  // interfaceMethodType: Runnable.run 의 선언 시그니처
            m1,  // implementation (반드시 direct MH)
            MethodType.methodType(Void.TYPE) // dynamicMethodType
        ).getTarget().invoke() as Runnable
        r1.run()
        println("  [1] class = " + r1.javaClass)

        // 2
        val m2 = lk.findStatic(
            LambdaMetafactoryDemo::class.java,
            LambdaMetafactoryDemo::impl2.name,
            MethodType.methodType(Void.TYPE, String::class.java)
        )
        val f2 = LambdaMetafactory.metafactory(
            lk, "run",
            MethodType.methodType(Runnable::class.java, String::class.java),  // 캡처 인자가 factoryType 파라미터
            MethodType.methodType(Void.TYPE),
            m2,
            MethodType.methodType(Void.TYPE)
        ).getTarget()
        (f2.invoke("captured-value") as Runnable).run()
        println("  [2] 서로 다른 인스턴스? " + (f2.invoke("a") !== f2.invoke("b")))

        // 3: samMethodType 은 소거형 (Object)Object, dynamicMethodType 은 실제형 (String)Integer
        val m3 = lk.findStatic(
            LambdaMetafactoryDemo::class.java,
            LambdaMetafactoryDemo::impl3.name,
            MethodType.methodType(Int::class.java, String::class.java)
        )
        val fn = LambdaMetafactory.metafactory(
            lk, Function<*,*>::apply.name,
            MethodType.methodType(Function::class.java),
            MethodType.methodType(Any::class.java, Any::class.java),  // ← 소거형
            m3,
            MethodType.methodType(Int::class.java, String::class.java) // ← 실제형, 여기서 checkcast 삽입
        ).getTarget().invoke() as Function<String?, Int?>
        println("  [3] fn.apply(\"abcde\") = " + fn.apply("abcde"))

        // 3b: dynamicMethodType 을 대충 소거형으로 주면 metafactory 가 바로 거부한다
        try {
            LambdaMetafactory.metafactory(
                lk, Function<*,*>::apply.name,
                MethodType.methodType(Function::class.java),
                MethodType.methodType(Any::class.java, Any::class.java),
                m3,
                MethodType.methodType(Any::class.java, Any::class.java)
            ) // impl 과 안 맞음
            println("  [3b] ?? 통과해버림")
        } catch (e: LambdaConversionException) {
            println("  [3b] 링크 시점 거부: " + e.message)
        }

        // 3c: 런타임 타입 위반은 생성된 checkcast 가 잡는다
        try {
            (fn as Function<*, *> as Function<Any?, Any?>).apply(42)
        } catch (e: ClassCastException) {
            println("  [3c] 호출 시점 CCE: " + e.message)
        }

        // 4: findVirtual 핸들을 implMethod 로 주고, 캡처값 = 리시버
        val exec = MethodType.methodType(Void.TYPE, Thread::class.java, Runnable::class.java)
        val foreignExecute = lk.findVirtual(Foreign::class.java, Foreign::execute.name, exec)
        val toMine = LambdaMetafactory.metafactory(
            lk, "execute",
            MethodType.methodType(MyProxy::class.java, Foreign::class.java),  // Foreign 을 캡처 → MyProxy 생성
            exec, foreignExecute, exec
        ).dynamicInvoker().asType(MethodType.methodType(MyProxy::class.java, Any::class.java)) // 타입 소거
        val p = toMine.invokeExact(ForeignImpl() as Any) as MyProxy
        p.execute(null, null)

        // 함정: implMethod 가 direct 가 아니면 실패
        try {
            LambdaMetafactory.metafactory(
                lk, Runnable::run.name,
                MethodType.methodType(Runnable::class.java),
                MethodType.methodType(Void.TYPE),
                m2.bindTo("x"),  // ← bindTo 로 변형됨 = non-direct
                MethodType.methodType(Void.TYPE)
            )
        } catch (t: Throwable) {
            println("  [!] non-direct implMethod: " + t.javaClass.getSimpleName() + ": " + t.message)
        }
    }

    // ---- 4. Micronaut 스타일: implMethod가 findVirtual, 캡처값이 리시버 ----
    internal interface MyProxy {
        fun execute(t: Thread?, r: Runnable?)
    } // 내 인터페이스

    interface Foreign {
        fun execute(t: Thread?, r: Runnable?)
    } // "JDK쪽" 인터페이스 역할

    class ForeignImpl : Foreign {
        override fun execute(t: Thread?, r: Runnable?) {
            println("  [4] ForeignImpl.execute")
        }
    }
}
# ----------- Validation Section with Chunking -----------
def chunk_text(text, max_chars=12000):
    """Split text into chunks under max_chars."""
    return [text[i:i+max_chars] for i in range(0, len(text), max_chars)]

if st.button("🚀 Validate Conversion"):
    if (informatica_file or datastage_file) and pyspark_file:
        st.info("⏳ Validating conversion... please wait.")

        # Read contents
        input_content = ""
        if informatica_file:
            input_content = informatica_file.read().decode("utf-8", errors="ignore")
        elif datastage_file:
            input_content = datastage_file.read().decode("utf-8", errors="ignore")

        pyspark_content = pyspark_file.read().decode("utf-8", errors="ignore")

        try:
            # --- Step 1: Chunk both files ---
            etl_chunks = chunk_text(input_content)
            pyspark_chunks = chunk_text(pyspark_content)

            validation_parts = []

            # --- Step 2: Validate chunk by chunk ---
            for i in range(max(len(etl_chunks), len(pyspark_chunks))):
                etl_chunk = etl_chunks[i] if i < len(etl_chunks) else ""
                pyspark_chunk = pyspark_chunks[i] if i < len(pyspark_chunks) else ""

                validation_prompt = f"""
                You are validating ETL to PySpark conversion (Part {i+1}).

                ETL Input (chunk {i+1}):
                {etl_chunk}

                PySpark Output (chunk {i+1}):
                {pyspark_chunk}

                Validate correctness for this chunk.
                Provide sections:
                - ✅ Correct parts
                - ⚠️ Potential issues
                - ❌ Missing logic
                - 💡 Suggested improvements
                """

                response = client.chat.completions.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": validation_prompt}],
                    temperature=0
                )

                validation_parts.append(response.choices[0].message.content.strip())

            # --- Merge all validation reports ---
            validation_report = "\n\n".join(validation_parts)
            st.success("✅ Validation Completed")
            st.markdown("### 📝 Validation Report")
            st.write(validation_report)

            # --- Step 3: Ask LLM to correct PySpark (send only cleaned + truncated version) ---
            correction_prompt = f"""
            Based on the ETL input and PySpark output, rewrite the PySpark code so that it
            fully and correctly implements the ETL logic.

            IMPORTANT:
            - Return only the corrected PySpark code.
            - If the original file is already correct, return the same code unchanged.
            """

            correction_response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are an expert PySpark converter."},
                    {"role": "user", "content": f"ETL Input (first chunk):\n{etl_chunks[0]}\n\nPySpark Output (first chunk):\n{pyspark_chunks[0]}\n\n{correction_prompt}"}
                ],
                temperature=0
            )

            corrected_pyspark = correction_response.choices[0].message.content.strip()

        except Exception as e:
            st.error(f"Error during validation: {e}")
    else:
        st.warning("⚠️ Please upload both an ETL file (Informatica/Datastage) and a PySpark file.")
